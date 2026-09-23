//! 分类树构建（方案 v3.1 §5.1）。
//!
//! 实测确认的唯一可用路径：`/v1/universe/groups/` 给 1 000 个裸 ID →
//! 逐个 `/v1/universe/groups/{id}` 拿 `category_id + types[]` →
//! 名字用 `POST /v1/universe/names/` 批量解析。
//!
//! 三条踩过的坑写进代码里：
//! - `/v1/markets/categories` 已 404、`/v2/markets/groups/{id}` 的 `types` 恒为空数组，
//!   所以不能走"市场接口"那条路；`/v1/universe/categories/` 也只给裸 ID。
//! - `universe/names` 是**全批要么成功要么整批 404**（实测混进一个不存在的 ID 就全批失败，
//!   还照扣 5 令牌），所以 ID 必须都来自 `groups/{id}` 的返回值。
//! - 组详情里有 `published: false`（实测 group 10 Stargate 即为 false），
//!   不剔除的话树里会混进上千个玩家在市场中根本看不见的类型。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use serde::Deserialize;

use crate::catalog::{batches, lookup_names, BATCH_SIZE};
use crate::error::Result;
use crate::esi::EsiClient;
use crate::store::Db;

#[derive(Debug, Clone, Deserialize)]
pub struct GroupDetail {
    pub group_id: u32,
    pub category_id: u32,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub published: bool,
    #[serde(default)]
    pub types: Vec<u32>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TreeStats {
    pub groups: usize,
    pub published_groups: usize,
    pub types: usize,
    pub names_resolved: usize,
    pub categories: usize,
    pub failed_groups: usize,
    pub elapsed: Duration,
}

/// 全部 inventory group ID（实测 1 000 个）。
pub async fn group_ids(client: &EsiClient) -> Result<Vec<u32>> {
    client.get_json("/v1/universe/groups/").await
}

pub async fn fetch_group(client: &EsiClient, group_id: u32) -> Result<GroupDetail> {
    client
        .get_json(&format!("/v1/universe/groups/{group_id}"))
        .await
}

/// `GET /v1/universe/categories/{id}`。**分类名只能从这里取**：
/// 实测分类 ID 与类型 ID 在数字空间上重叠 —— `POST /v1/universe/names/` 传 `[2]`
/// 返回的是 `inventory_type 2 "Corporation"`（一种金属板），而不是分类 2 "Asteroids"。
/// 用 names 解分类名不会报错，只会静默写进一批错名字。
#[derive(Debug, Clone, Deserialize)]
pub struct CategoryDetail {
    pub category_id: u32,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub published: bool,
    #[serde(default)]
    pub groups: Vec<u32>,
}

/// 全部 inventory category ID（实测 46 个）。
pub async fn category_ids(client: &EsiClient) -> Result<Vec<u32>> {
    client.get_json("/v1/universe/categories/").await
}

/// 46 个请求，成本可忽略。返回写入条数。
pub async fn build_categories(client: &EsiClient, db: &Db) -> Result<usize> {
    let ids = category_ids(client).await?;
    let fetched: Vec<Option<CategoryDetail>> = stream::iter(ids)
        .map(|cid| async move {
            client
                .get_json::<CategoryDetail>(&format!("/v1/universe/categories/{cid}"))
                .await
                .ok()
        })
        .buffer_unordered(client.config().concurrency)
        .collect()
        .await;
    let details: Vec<CategoryDetail> = fetched.into_iter().flatten().collect();
    db.write_categories(&details)
}

/// 一次性建树。再跑一次等于拿 ESI 的 24 h 缓存做 diff。
pub async fn build_tree(client: &EsiClient, db: &Db) -> Result<TreeStats> {
    let t0 = Instant::now();
    let ids = group_ids(client).await?;
    tracing::info!("分类树：{} 个组待拉取", ids.len());

    let fetched: Vec<Option<GroupDetail>> = stream::iter(ids)
        .map(|gid| async move { fetch_group(client, gid).await.ok() })
        .buffer_unordered(client.config().concurrency)
        .collect()
        .await;

    let failed_groups = fetched.iter().filter(|g| g.is_none()).count();
    let details: Vec<GroupDetail> = fetched.into_iter().flatten().collect();
    let published = details.iter().filter(|g| g.published).count();
    db.write_groups(&details)?;

    // 类型名只解析 published 组 —— 实测未发布组占相当比例，能省掉大量请求。
    let mut group_of: HashMap<u32, u32> = HashMap::new();
    for g in details.iter().filter(|g| g.published) {
        for t in &g.types {
            group_of.insert(*t, g.group_id);
        }
    }
    let type_ids: Vec<u64> = group_of.keys().copied().map(u64::from).collect();
    let names_resolved = resolve_in_batches(client, db, &type_ids, &group_of, "inventory_type").await?;

    let categories = build_categories(client, db).await?;

    let stats = TreeStats {
        groups: details.len(),
        published_groups: published,
        types: group_of.len(),
        names_resolved,
        categories,
        failed_groups,
        elapsed: t0.elapsed(),
    };
    tracing::info!(
        "分类树完成：{} 组（{} 已发布，{} 组拉取失败）/ {} 类型 / 解名 {} / 分类 {} / {:.1}s",
        stats.groups,
        stats.published_groups,
        stats.failed_groups,
        stats.types,
        stats.names_resolved,
        stats.categories,
        stats.elapsed.as_secs_f64()
    );
    Ok(stats)
}

/// 批量解名。整批 404 时降级为逐个查 —— 一个坏 ID 不该让 200 个名字丢失。
async fn resolve_in_batches(
    client: &EsiClient,
    db: &Db,
    ids: &[u64],
    group_of: &HashMap<u32, u32>,
    category: &str,
) -> Result<usize> {
    let mut found: Vec<(u64, String)> = Vec::new();
    for batch in batches(ids, BATCH_SIZE) {
        match lookup_names(client, &batch).await {
            Ok(named) => collect(&mut found, &named, category),
            Err(e) => {
                tracing::debug!("批量解名 {} 个失败，退回逐个：{e}", batch.len());
                for id in batch {
                    if let Ok(named) = lookup_names(client, &[id]).await {
                        collect(&mut found, &named, category);
                    }
                }
            }
        }
    }
    db.write_types(&found, group_of)
}

fn collect(out: &mut Vec<(u64, String)>, named: &[crate::catalog::Named], category: &str) {
    out.extend(
        named
            .iter()
            .filter(|n| n.category == category)
            .map(|n| (n.id as u64, n.name.clone())),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECORDED_GROUP_10: &str = r#"{"category_id":2,"group_id":10,"name":"Stargate","published":false,"types":[16,17,3873,3874,3875,3876,3877,12292,29624,29625,29626,29627,29628,29629,29630,29631,29632,29633,29634,29635,56317,57760,77918,77921,78264,78265,78266,93250,93251,93252,93638,93639]}"#;

    #[test]
    fn parses_the_recorded_group_detail() {
        let g: GroupDetail = serde_json::from_str(RECORDED_GROUP_10).unwrap();
        assert_eq!(g.group_id, 10);
        assert_eq!(g.category_id, 2);
        assert_eq!(g.name.as_deref(), Some("Stargate"));
        assert_eq!(g.types.len(), 32);
        assert!(!g.published, "实测 group 10 未发布，必须被树过滤");
    }

    #[test]
    fn missing_optional_fields_default_instead_of_failing() {
        let g: GroupDetail = serde_json::from_str(r#"{"group_id":5,"category_id":7}"#).unwrap();
        assert!(!g.published);
        assert!(g.types.is_empty());
        assert!(g.name.is_none());
    }

    #[test]
    fn parses_the_recorded_category_detail() {
        // /v1/universe/categories/10 的原样响应。
        let c: CategoryDetail =
            serde_json::from_str(r#"{"category_id":10,"groups":[94,95],"name":"Trading","published":false}"#)
                .unwrap();
        assert_eq!(c.category_id, 10);
        assert_eq!(c.name.as_deref(), Some("Trading"));
        assert_eq!(c.groups, vec![94, 95]);
        assert!(!c.published);
    }

    #[test]
    fn category_ids_have_no_name_field_so_they_cannot_come_from_names() {
        // 实测：把分类 ID 2 丢给 /universe/names，返回的是 inventory_type "Corporation"
        // （一种金属板），而不是分类 "Asteroids" —— 数字重叠，且不会报错。
        let as_types: Vec<crate::catalog::Named> = serde_json::from_str(
            r#"[{"category":"inventory_type","id":2,"name":"Corporation"}]"#,
        )
        .unwrap();
        assert_eq!(as_types[0].category, "inventory_type");
        assert_ne!(as_types[0].name, "Asteroids", "证明 names 给不出分类名");
    }

    #[test]
    fn collect_keeps_only_the_requested_category() {
        let named: Vec<crate::catalog::Named> = serde_json::from_str(
            r#"[{"category":"inventory_type","id":34,"name":"Tritanium"},{"category":"corporation","id":98022090,"name":"Nyx Inc."},{"category":"system","id":30000142,"name":"Jita"}]"#,
        )
        .unwrap();
        let mut out = Vec::new();
        collect(&mut out, &named, "inventory_type");
        assert_eq!(out, vec![(34u64, "Tritanium".to_string())]);
    }

    #[test]
    fn group_of_map_covers_every_type_once() {
        let details = vec![
            GroupDetail {
                group_id: 18,
                category_id: 5,
                name: None,
                published: true,
                types: vec![34, 35, 36],
            },
            GroupDetail {
                group_id: 20,
                category_id: 5,
                name: None,
                published: false,
                types: vec![16],
            },
        ];
        let mut group_of: HashMap<u32, u32> = HashMap::new();
        for g in details.iter().filter(|g| g.published) {
            for t in &g.types {
                group_of.insert(*t, g.group_id);
            }
        }
        assert_eq!(group_of.len(), 3, "未发布组的类型不得进树");
        assert_eq!(group_of.get(&16), None);
        assert_eq!(group_of.get(&34), Some(&18));
    }
}
