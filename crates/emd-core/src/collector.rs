//! 进程级采集编排：单实例锁分"采集者/查看者"，采集者在 T1 轮间挂 T3 历史回填。
//!
//! 为什么要锁：`emd-daemon serve` 与 Tauri 壳此前各跑一套 Scheduler，同打 ESI 会
//! 令牌翻倍、`station_orders` 整表替换互相打架（方案 §7 常驻 + 交接的"双采集器"缺口）。

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;

/// 持有 `collector.lock` 的独占句柄；drop 即释放（进程崩溃由 OS 回收句柄）。
///
/// 锁文件留在盘上无妨（内容为空）；真正的互斥来自 OS 的文件锁，而非文件是否存在。
pub struct InstanceLock {
    // 句柄必须在锁生命周期内存活，字段本身不被读取。
    _file: File,
    path: PathBuf,
}

impl InstanceLock {
    /// 抢到返回 Some；已被别的进程持有返回 None。非阻塞：拿不到立刻返回，
    /// 调用方据此进入"查看者"模式，不排队等待（两个采集者本就不该并存）。
    pub fn acquire(data_dir: &Path) -> Option<Self> {
        let path = data_dir.join("collector.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.try_lock_exclusive().ok().map(|_| Self { _file: file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // 释放失败也无妨：句柄随进程关闭时 OS 会解锁。显式解锁只为同进程内可及时重取。
        let _ = fs2::FileExt::unlock(&self._file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "emd-lock-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn second_holder_cannot_acquire_the_same_lock() {
        let dir = tmp_dir("dup");
        let first = InstanceLock::acquire(&dir).expect("第一把应成功");
        assert!(
            InstanceLock::acquire(&dir).is_none(),
            "同目录第二把锁必须失败（这就是分'采集者/查看者'的依据）"
        );
        drop(first);
        assert!(
            InstanceLock::acquire(&dir).is_some(),
            "释放后应可重取（崩溃残留的锁文件不挡路）"
        );
    }

    #[test]
    fn lock_reports_its_path_under_data_dir() {
        let dir = tmp_dir("path");
        let guard = InstanceLock::acquire(&dir).unwrap();
        assert_eq!(guard.path(), dir.join("collector.lock").as_path());
    }
}
