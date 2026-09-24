//! 令牌存储。`refresh_token` 只进这里（keyring）与内存，DB 不落（Global Constraints）。
//! trait 是为了让测试与"用户收回私有数据豁免后回退本地"的场景能替换实现。

use crate::error::{Error, Result};
use crate::sso::token::TokenSet;

/// 令牌的读写契约。`load` 返回 `Ok(None)` 只表示一件事：**当前没有可用令牌**
/// （从未登录 / 已退出 / 凭据库不可用 / 凭据内容已损坏），调用方据此走登录流程即可，
/// 无需分辨原因。
pub trait TokenStore: Send + Sync {
    fn load(&self) -> Result<Option<TokenSet>>;
    fn save(&self, t: &TokenSet) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

/// 进程内实现：只用于测试与"本轮不想落盘"的场景，进程退出即丢。
///
/// 用 `Mutex` 而非 `RwLock`：读也要 clone 整个 `TokenSet`，读写代价同级，
/// 争用概率又极低，`RwLock` 只多一份复杂度。
#[derive(Default)]
pub struct MemoryTokenStore(std::sync::Mutex<Option<TokenSet>>);

impl TokenStore for MemoryTokenStore {
    fn load(&self) -> Result<Option<TokenSet>> {
        // 中毒即"持锁线程 panic 过"：令牌是一份无跨字段不变量的快照，
        // 取回内层值继续用比让整个进程炸掉更合理（与 esi/client.rs 的取法一致）。
        Ok(self.0.lock().unwrap_or_else(|p| p.into_inner()).clone())
    }

    fn save(&self, t: &TokenSet) -> Result<()> {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = Some(t.clone());
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        // 幂等：本来就没有也算成功，调用方不必先查再删。
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = None;
        Ok(())
    }
}

/// 系统凭据库（Windows 凭据管理器）实现：令牌与用户账户绑定，不落应用数据目录。
pub struct KeyringTokenStore {
    service: String,
    account: String,
}

impl KeyringTokenStore {
    pub fn new(service: impl Into<String>, account: impl Into<String>) -> Self {
        Self { service: service.into(), account: account.into() }
    }

    fn entry(&self) -> std::result::Result<keyring::Entry, keyring::Error> {
        keyring::Entry::new(&self.service, &self.account)
    }
}

impl TokenStore for KeyringTokenStore {
    /// **凭据库读不到 ≠ 出错**：读不到、打不开、内容坏了，一律是"没登录"，
    /// 全部返回 `Ok(None)`，只留 `debug!` 线索。
    ///
    /// 唯一不能这么做的是写（见 `save`）——把读失败报成 `Err` 会让"首次登录"
    /// 这个最常见的路径直接崩，而写失败才是真的没存下。
    fn load(&self) -> Result<Option<TokenSet>> {
        // 构造也可能失败（条目属性非法、长度超限），同样按"没登录"降级。
        let entry = match self.entry() {
            Ok(e) => e,
            Err(e) => {
                tracing::debug!("凭据条目无法构造（{e}），按未登录处理");
                return Ok(None);
            }
        };

        let json = match entry.get_password() {
            Ok(j) => j,
            Err(keyring::Error::NoEntry) => {
                tracing::debug!("凭据库无此条目，按未登录处理");
                return Ok(None);
            }
            // 无凭据服务、拒绝访问、平台故障……都不是"令牌无效"，而是"这次读不出来"。
            // 用户下次登录成功即可自愈，因此不能在这里把登录入口堵死。
            Err(e) => {
                tracing::debug!("凭据库不可用（{e}），按未登录处理");
                return Ok(None);
            }
        };

        // 读到了但解不开（换过序列化格式、凭据被手工改过）：坏数据等价于未登录，
        // 反正也拿去发不了请求。
        match serde_json::from_str(&json) {
            Ok(t) => Ok(Some(t)),
            Err(e) => {
                tracing::debug!("凭据内容不是合法令牌（{e}），按未登录处理");
                Ok(None)
            }
        }
    }

    /// 写失败如实上抛：令牌没存下就是没存下，若在这里静默成功，
    /// 下次 `load` 又是 `Ok(None)`，用户会看到"登录成功却一直要重新登录"。
    /// 报错文案不含令牌原文（`keyring::Error` 的 Display 也不回显写入内容）。
    fn save(&self, t: &TokenSet) -> Result<()> {
        let json = serde_json::to_string(t)
            .map_err(|e| Error::Config(format!("令牌序列化失败，未写入凭据库：{e}")))?;
        let entry = self
            .entry()
            .map_err(|e| Error::Config(format!("无法打开凭据条目，令牌未写入：{e}")))?;
        entry
            .set_password(&json)
            .map_err(|e| Error::Config(format!("写入系统凭据库失败：{e}")))?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let entry = self
            .entry()
            .map_err(|e| Error::Config(format!("无法打开凭据条目，无法清除令牌：{e}")))?;
        match entry.delete_credential() {
            Ok(()) => Ok(()),
            // 幂等：本来就没有 = 已经是"退出"状态，退出登录不该因为"没登录过"而报错。
            Err(keyring::Error::NoEntry) => {
                tracing::debug!("凭据库无此条目，无需清除");
                Ok(())
            }
            Err(e) => Err(Error::Config(format!("清除系统凭据库条目失败：{e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_roundtrips_and_clears() {
        let s = MemoryTokenStore::default();
        assert!(s.load().unwrap().is_none());
        let t = TokenSet { access_token: "AT".into(), refresh_token: "RT".into(), expires_at: 9 };
        s.save(&t).unwrap();
        assert_eq!(s.load().unwrap().unwrap(), t);
        s.clear().unwrap();
        assert!(s.load().unwrap().is_none(), "退出登录必须真的清掉");
    }

    /// keyring 的行为按"抽象契约"测，不打真实 Windows 凭据库
    /// ——CI/无桌面环境没有凭据服务，打真库会让测试变脆。
    #[test]
    fn keyring_store_degrades_to_none_when_backend_is_unavailable() {
        // 用不存在的服务名构造，load 应回 Ok(None) 而不是 panic/Err
        // （凭据库不可用 = 没登录过，不是致命错误）
        let s = KeyringTokenStore::new("EveMarketDeskTest__nope__", "prod");
        match s.load() {
            Ok(None) => {}
            Ok(Some(_)) => panic!("不该读到令牌"),
            Err(e) => panic!("凭据库不可用应降级为 None，实际报错：{e}"),
        }
    }
}
