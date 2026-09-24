//! SSO 回环回调监听：登录时临时起一个本地端口，只收浏览器重定向回来的那一个 GET。
//!
//! 用 `tiny_http` 而不是手写 HTTP 解析：请求行/头部/大小写/分块这些细节自己写一定会漏，
//! 而这里只需要"收一个请求、取 query、回一个页面"。也不引 async 服务端框架——
//! 登录是用户手点的一次性动作，为它拉一整套运行时不合算。
//!
//! 生命周期：监听器的所有权属于一次登录尝试，`login` 把它移进阻塞任务，
//! 成功、state 不符、超时三条路径都在那里把它丢掉，端口不会留着。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::sso::verify_callback;

/// 回环回调监听器。**只在 `login` 期间存在**，不要往外传（传出去就会漏端口）。
pub struct CallbackListener {
    server: tiny_http::Server,
}

impl CallbackListener {
    /// 绑定 `127.0.0.1:{port}`。`port == 0` 时由 OS 分配（测试与"让系统挑空闲端口"用）。
    ///
    /// 只绑回环地址：授权码是登录凭据，不该出现在局域网里；EVE 也要求
    /// redirect_uri 与开发者后台注册值精确一致（回环地址同样要注册）。
    pub fn bind(port: u16) -> Result<Self> {
        let addr = format!("127.0.0.1:{port}");
        let server = tiny_http::Server::http(addr.as_str())
            .map_err(|e| Error::Config(format!("无法绑定 SSO 回调端口 {addr}：{e}")))?;
        Ok(Self { server })
    }

    /// 实际监听地址。绑 0 端口时**必须**从这里取真实端口（redirect_uri 要用它）。
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.server
            .server_addr()
            .to_ip()
            .ok_or_else(|| Error::Config("SSO 回调监听地址不是 TCP 地址".into()))
    }

    /// 等一个回调并取出 `code`。`timeout` 是**整次尝试**的上限（不是每个请求各一份），
    /// 用户不完成授权时在此报超时。
    ///
    /// state 校验交给 [`verify_callback`]（T1 的纯函数），本文件不重复实现——
    /// 校验规则只有一处，改动不会有第二条漏掉的路径。
    pub fn wait_for_code(&self, timeout: Duration, expected_state: &str) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(timeout_error(timeout));
            }
            let req = match self.server.recv_timeout(left) {
                Ok(Some(r)) => r,
                Ok(None) => return Err(timeout_error(timeout)),
                Err(e) => return Err(Error::Config(format!("SSO 回调监听失败：{e}"))),
            };

            let query = req
                .url()
                .split_once('?')
                .map(|(_, q)| q.to_string())
                .unwrap_or_default();

            // 浏览器会为回调页顺手请求 /favicon.ico 之类的路径：那不是回调。
            // 若在此判失败，用户一次正常登录会被一条杂请求搅掉。
            if !looks_like_callback(&query) {
                let _ = req.respond(tiny_http::Response::empty(404));
                continue;
            }

            let outcome = verify_callback(&query, expected_state);
            let page = if outcome.is_ok() {
                "登录成功，可以关闭此页面。"
            } else {
                "登录失败，请回到应用重新发起登录。"
            };
            // 回页失败（用户提前关了标签页）不影响本次登录的结果判定，忽略即可。
            let _ = req.respond(tiny_http::Response::from_string(page));
            return outcome;
        }
    }
}

/// 回调请求的判据：query 里出现 code/state/error 三者之一。
/// 只按"有没有 code"判会把 `error=access_denied`（用户点了拒绝）误当杂请求而一直等到超时。
fn looks_like_callback(query: &str) -> bool {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('=').map(|(k, _)| k))
        .any(|k| matches!(k, "code" | "state" | "error"))
}

fn timeout_error(timeout: Duration) -> Error {
    Error::Config(format!("SSO 登录超时（等待回调 {:?} 无有效结果）", timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// loopback 监听器：真起一个本地端口，自己发一个 GET 过去，断言能捞出 code。
    /// 用 0 端口让 OS 分配，避免测试间抢端口。
    #[test]
    fn listener_extracts_code_from_a_real_local_request() {
        let l = CallbackListener::bind(0).unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            // 用裸 TcpStream 发一个最小 HTTP GET，避免为测试再引 HTTP 客户端
            use std::io::Write;
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            let _ = s.write_all(b"GET /callback?code=THE_CODE&state=ST HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        });
        let code = l.wait_for_code(std::time::Duration::from_secs(5), "ST").unwrap();
        assert_eq!(code, "THE_CODE");
    }

    #[test]
    fn listener_rejects_wrong_state() {
        let l = CallbackListener::bind(0).unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::Write;
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            let _ = s.write_all(b"GET /callback?code=C&state=EVIL HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        });
        assert!(l.wait_for_code(std::time::Duration::from_secs(5), "ST").is_err());
    }

    #[test]
    fn listener_times_out_when_nobody_calls_back() {
        let l = CallbackListener::bind(0).unwrap();
        let e = l.wait_for_code(std::time::Duration::from_millis(200), "ST").unwrap_err();
        assert!(e.to_string().contains("超时"), "{e}");
    }
}
