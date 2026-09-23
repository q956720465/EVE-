//! 站点字典：把 `location_id` 变成人能读的名字。
//!
//! 实测契约（2026-09-23，`POST /v1/universe/names/`）：
//! - 请求体是**裸数组** `[60003760, ...]`；包成 `{"ids":[...]}` 会 400 且照扣 5 令牌。
//! - 响应是 `[{"category":"station","id":60003760,"name":"Jita IV - Moon 4 - ..."}]`，键全小写。
//! - 13 位玩家结构 ID 会让整个请求 400（`failed to coerce value into int32`）
//!   → 必须在发送前过滤掉，否则一颗老鼠屎坏掉一批。

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::esi::EsiClient;
use crate::market::LocationKind;
use crate::store::Db;

/// 单批上限。ESI 允许 1000，但本机到 ESI 单连接只有 ~10 KB/s，
/// 小批多次更稳，也便于失败时只重发一批。
pub const BATCH_SIZE: usize = 200;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Named {
    pub category: String,
    pub id: i64,
    pub name: String,
}

/// 只保留可安全发送的 ID（int32 范围内且是 NPC 站）。
pub fn sendable_ids(ids: &[u64]) -> Vec<u64> {
    ids.iter()
        .filter(|i| LocationKind::of(**i).tradable_publicly() && **i <= i32::MAX as u64)
        .copied()
        .collect()
}

pub fn batches<T: Clone>(items: &[T], size: usize) -> Vec<Vec<T>> {
    items.chunks(size.max(1)).map(|c| c.to_vec()).collect()
}

/// `POST /v1/universe/names/`。body 必须是裸数组 —— 见模块头的实测说明。
pub async fn lookup_names(client: &EsiClient, ids: &[u64]) -> Result<Vec<Named>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let body = serde_json::to_vec(ids).map_err(|e| Error::Config(format!("序列化 ID 失败: {e}")))?;
    let resp = client.post_json("/v1/universe/names/", &body).await?;
    let parsed: Vec<Named> =
        serde_json::from_slice(&resp).map_err(|source| Error::Parse {
            url: "/v1/universe/names/".into(),
            source,
        })?;
    Ok(parsed)
}

/// `POST /v1/universe/ids/` 的响应按**类别分组**，且组内对象只有 `id`/`name`
/// —— 没有 `category` 字段（分组键本身就是类别），所以不能复用 `/universe/names`
/// 的 `Named`。同一个词往往同时命中物品、军团、角色与星系（实测 "Nyx" 同时给出
/// inventory_type 23913 与 corporation "Nyx Inc."），必须只取需要的类别。
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Identified {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct IdsResponse {
    #[serde(default)]
    pub inventory_types: Vec<Identified>,
    #[serde(default)]
    pub systems: Vec<Identified>,
    #[serde(default)]
    pub regions: Vec<Identified>,
}

impl IdsResponse {
    pub fn type_ids(&self) -> Vec<u32> {
        self.inventory_types.iter().map(|n| n.id as u32).collect()
    }
}

/// 名称 → 类型 ID（公开接口，不需要 OAuth；`/characters/{id}/search` 才要）。
pub async fn search(client: &EsiClient, words: &[&str]) -> Result<IdsResponse> {
    let body = serde_json::to_vec(words).map_err(|e| Error::Config(format!("序列化失败: {e}")))?;
    let resp = client
        .post_json("/v1/universe/ids/", &body)
        .await
        .or_else(|e| match e {
            // 一个词都没命中时 ESI 返回 404 —— 这是"查无此物"，不是错误。
            Error::Status { status: 404, .. } => Ok(Vec::new()),
            other => Err(other),
        })?;
    serde_json::from_slice(&resp).map_err(|source| Error::Parse {
        url: "/v1/universe/ids/".into(),
        source,
    })
}

/// 把库里所有还没名字的 NPC 站补齐。返回解析成功的条数。
///
/// 一轮全量后大约会攒出几十个未名站点，一次搞定就永久缓存，
/// 所以这个函数只在启动和每日维护时调用，不进 6 分钟循环。
pub async fn resolve_missing(client: &EsiClient, db: &Db, limit: u32) -> Result<usize> {
    let pending = db.unnamed_npc_stations(limit)?;
    let sendable = sendable_ids(&pending);
    let mut done = 0usize;

    for batch in batches(&sendable, BATCH_SIZE) {
        match lookup_names(client, &batch).await {
            Ok(named) => {
                for n in named {
                    if n.category != "station" {
                        continue;
                    }
                    db.name_station(n.id as u64, &n.name, None)?;
                    done += 1;
                }
            }
            // 一批里有一个非法 ID 就整批 400。降级成逐个查，别因为一个坏 ID 丢掉全部。
            Err(e) => {
                tracing::warn!("批量站点解析失败（{} 个 ID），退回逐个：{e}", batch.len());
                for id in batch {
                    if let Ok(named) = lookup_names(client, &[id]).await {
                        for n in named.iter().filter(|n| n.category == "station") {
                            db.name_station(n.id as u64, &n.name, None)?;
                            done += 1;
                        }
                    }
                }
            }
        }
    }
    Ok(done)
}

/// `universe/stations/{id}` 才能给 system_id（公开可读）。目前只在需要星系信息时单独取。
#[derive(Debug, Clone, Deserialize)]
pub struct StationDetail {
    pub name: String,
    #[serde(rename = "system_id")]
    pub system_id: u32,
    #[serde(default)]
    pub station_id: Option<u64>,
}

#[allow(dead_code)]
pub async fn station_detail(client: &EsiClient, location_id: u64) -> Result<StationDetail> {
    client
        .get_json(&format!("/v1/universe/stations/{location_id}"))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECORDED: &str = r#"[{"category":"station","id":60003760,"name":"Jita IV - Moon 4 - Caldari Navy Assembly Plant"},{"category":"station","id":60015157,"name":"Kisogo VII - AIR Laboratories"},{"category":"inventory_type","id":34,"name":"Tritanium"},{"category":"region","id":10000002,"name":"The Forge"}]"#;

    #[test]
    fn ids_response_keeps_only_inventory_types() {
        // 2026-09-23 实测原样录制：同一个词命中了物品、角色、军团与星系。
        const RECORDED: &str = r#"{"alliances":[{"id":99005382,"name":"Jita Holding Inc."}],"characters":[{"id":243070982,"name":"Tritanium"},{"id":786259038,"name":"Nyx"},{"id":153665635,"name":"Isotope"}],"corporations":[{"id":98022090,"name":"Nyx Inc."},{"id":383768304,"name":"jion ss Corp"}],"inventory_types":[{"id":34,"name":"Tritanium"},{"id":23913,"name":"Nyx"}],"systems":[{"id":30000142,"name":"Jita"}]}"#;
        let r: IdsResponse = serde_json::from_str(RECORDED).unwrap();
        assert_eq!(r.type_ids(), vec![34, 23913]);
        assert_eq!(r.systems[0].id, 30000142);
    }

    #[test]
    fn empty_ids_response_is_not_an_error() {
        let r: IdsResponse = serde_json::from_str("{}").unwrap();
        assert!(r.type_ids().is_empty());
    }

    #[test]
    fn parses_the_recorded_response_verbatim() {
        let v: Vec<Named> = serde_json::from_str(RECORDED).expect("真实响应必须能解析");
        assert_eq!(v.len(), 4);
        assert_eq!(v[0].id, 60003760);
        assert_eq!(v[0].category, "station");
        assert!(v[0].name.starts_with("Jita IV - Moon 4"));
        assert_eq!(v[3].name, "The Forge");
    }

    #[test]
    fn only_station_category_is_sendable_as_a_location() {
        let named: Vec<Named> = serde_json::from_str(RECORDED).unwrap();
        let stations: Vec<i64> = named
            .iter()
            .filter(|n| n.category == "station")
            .map(|n| n.id)
            .collect();
        assert_eq!(stations, vec![60003760, 60015157]);
    }

    #[test]
    fn player_structures_never_go_into_the_request() {
        // 13 位 ID 会让整批 400（实测），必须提前剔掉。
        let ids = vec![60003760u64, 1044752365771, 60015157, 1234567890123];
        assert_eq!(sendable_ids(&ids), vec![60003760, 60015157]);
    }

    #[test]
    fn oversized_ids_are_dropped_too() {
        let ids = vec![60003760u64, 3_000_000_000];
        assert_eq!(sendable_ids(&ids), vec![60003760]);
    }

    #[test]
    fn batching_covers_everything_without_overlap() {
        let ids: Vec<u64> = (0..450).map(|i| 60_000_000 + i).collect();
        let b = batches(&ids, BATCH_SIZE);
        assert_eq!(b.len(), 3);
        assert_eq!(b.iter().map(|x| x.len()).sum::<usize>(), 450);
        assert_eq!(b[2].len(), 50);
    }

    #[test]
    fn batching_handles_zero_and_huge_sizes() {
        let ids = vec![1u64, 2, 3];
        assert_eq!(batches(&ids, 0).len(), 3, "size=0 不得死循环或丢数据");
        assert_eq!(batches(&ids, 999).len(), 1);
        assert!(batches(&[] as &[u64], 10).is_empty());
    }

    #[test]
    fn empty_id_list_short_circuits() {
        assert!(sendable_ids(&[]).is_empty());
    }
}
