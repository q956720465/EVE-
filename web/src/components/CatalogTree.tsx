import { useStore } from "../store";

export default function CatalogTree() {
  const tree = useStore((s) => s.tree);
  const expanded = useStore((s) => s.expanded);
  const toggle = useStore((s) => s.toggle);
  const groupId = useStore((s) => s.groupId);
  const selectGroup = useStore((s) => s.selectGroup);

  return (
    <div className="pane">
      <h3>分类树 · {tree.length} 个分类</h3>
      {tree.map((c) => {
        const open = expanded[c.category_id] ?? false;
        return (
          <div key={c.category_id}>
            <div className="cat" onClick={() => toggle(c.category_id)}>
              {open ? "▾" : "▸"} {c.name}
              {/* 裸数字看不懂是什么的数：直写成"N 个组"。 */}
              <span className="n"> · {c.groups.length} 个组</span>
            </div>
            {open &&
              c.groups.map((g) => (
                <div
                  key={g.group_id}
                  className={`grp ${groupId === g.group_id ? "on" : ""}`}
                  onClick={() => void selectGroup(g.group_id, g.name)}
                  title={`group_id ${g.group_id}`}
                >
                  <span>{g.name}</span>
                  <span className="n">{g.type_count} 个类型</span>
                </div>
              ))}
          </div>
        );
      })}
      {tree.length === 0 && (
        <div className="empty">
          分类树还没建好：壳启动时会自动建一次（实测 78 秒），建完这一栏会自己出现。
        </div>
      )}
    </div>
  );
}
