import { useState, useEffect, useCallback } from 'react';
import { BTreeVisualizer } from './components/BTreeVisualizer';
import { api, TreeNode, Stats, BTreeConfig } from './api';

function App() {
    const [dbOpen, setDbOpen] = useState(false);
    const [tree, setTree] = useState<TreeNode | null>(null);
    const [stats, setStats] = useState<Stats | null>(null);
    const [config, setConfig] = useState<BTreeConfig>({ maxLeafKeys: 4, maxInteriorKeys: 3 });
    const [key, setKey] = useState('');
    const [value, setValue] = useState('');
    const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
    const [loading, setLoading] = useState(false);

    const showToast = (message: string, type: 'success' | 'error') => {
        setToast({ message, type });
        setTimeout(() => setToast(null), 3000);
    };

    const refreshTree = useCallback(async () => {
        if (!dbOpen) return;
        try {
            const data = await api.getTree();
            setTree(data.tree || null);
            if (data.stats) {
                setStats({
                    // Subtract 1 because page 0 is the file header, not a B-tree node
                    pageCount: Math.max(0, data.stats.pageCount - 1),
                    bufferPoolSize: data.stats.bufferPoolSize,
                    treeHeight: data.stats.treeHeight,
                });
                setConfig(data.stats.btreeConfig);
            }
        } catch {
            // Ignore errors during refresh
        }
    }, [dbOpen]);

    useEffect(() => {
        if (dbOpen) {
            refreshTree();
        }
    }, [dbOpen, refreshTree]);

    const handleCreateDb = async () => {
        setLoading(true);
        try {
            await api.createDb({
                maxLeafKeys: config.maxLeafKeys,
                maxInteriorKeys: config.maxInteriorKeys,
            });
            setDbOpen(true);
            showToast('Database created!', 'success');
            await refreshTree();
        } catch (e) {
            showToast(`Failed to create database: ${e}`, 'error');
        }
        setLoading(false);
    };

    const handleCloseDb = async () => {
        try {
            await api.closeDb();
            setDbOpen(false);
            setTree(null);
            setStats(null);
            showToast('Database closed', 'success');
        } catch (e) {
            showToast(`Failed to close database: ${e}`, 'error');
        }
    };

    const handlePut = async () => {
        if (!key.trim()) {
            showToast('Key is required', 'error');
            return;
        }
        try {
            await api.put(key, value || key);
            showToast(`Inserted: ${key}`, 'success');
            setKey('');
            setValue('');
            await refreshTree();
        } catch (e) {
            showToast(`Insert failed: ${e}`, 'error');
        }
    };

    const handleDelete = async () => {
        if (!key.trim()) {
            showToast('Key is required', 'error');
            return;
        }
        try {
            await api.delete(key);
            showToast(`Deleted: ${key}`, 'success');
            setKey('');
            await refreshTree();
        } catch (e) {
            showToast(`Delete failed: ${e}`, 'error');
        }
    };

    const handleClear = async () => {
        try {
            await api.clear();
            showToast('Database cleared', 'success');
            await refreshTree();
        } catch (e) {
            showToast(`Clear failed: ${e}`, 'error');
        }
    };

    const handleQuickInsert = async (count: number) => {
        const pairs = Array.from({ length: count }, (_, i) => ({
            key: String(i + 1).padStart(3, '0'),
            value: `value_${i + 1}`,
        }));
        try {
            await api.bulkInsert(pairs);
            showToast(`Inserted ${count} keys`, 'success');
            await refreshTree();
        } catch (e) {
            showToast(`Bulk insert failed: ${e}`, 'error');
        }
    };

    const handleInsertLetters = async () => {
        const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
        const pairs = letters.map((l) => ({ key: l, value: l.toLowerCase() }));
        try {
            await api.bulkInsert(pairs);
            showToast('Inserted A-Z', 'success');
            await refreshTree();
        } catch (e) {
            showToast(`Insert failed: ${e}`, 'error');
        }
    };

    return (
        <div className="app">
            <header className="header">
                <h1>
                    🌳 <span>B-Tree</span> Visualizer
                </h1>
                {dbOpen && stats && (
                    <div style={{ display: 'flex', gap: '1.5rem', fontSize: '0.875rem' }}>
                        <span>Height: <strong>{stats.treeHeight}</strong></span>
                        <span>Pages: <strong>{stats.pageCount}</strong></span>
                        <span>Max Leaf Keys: <strong>{config.maxLeafKeys}</strong></span>
                        <span>Max Interior Keys: <strong>{config.maxInteriorKeys}</strong></span>
                    </div>
                )}
            </header>

            <main className="main">
                <aside className="sidebar">
                    {/* Configuration Section */}
                    <section className="section">
                        <h3>⚙️ Configuration</h3>
                        <div className="config-grid">
                            <div className="config-row">
                                <label>🌿 Leaf Node Limit</label>
                                <input
                                    type="number"
                                    min={2}
                                    max={20}
                                    value={config.maxLeafKeys}
                                    onChange={(e) => setConfig({ ...config, maxLeafKeys: parseInt(e.target.value) || 2 })}
                                    disabled={dbOpen}
                                />
                            </div>
                            <div className="config-row">
                                <label>🔀 Interior Node Limit</label>
                                <input
                                    type="number"
                                    min={2}
                                    max={20}
                                    value={config.maxInteriorKeys}
                                    onChange={(e) => setConfig({ ...config, maxInteriorKeys: parseInt(e.target.value) || 2 })}
                                    disabled={dbOpen}
                                />
                            </div>
                        </div>
                        <div style={{ marginTop: '1rem' }}>
                            {!dbOpen ? (
                                <button className="btn btn-primary btn-block" onClick={handleCreateDb} disabled={loading}>
                                    {loading ? 'Creating...' : '🚀 Create Database'}
                                </button>
                            ) : (
                                <button className="btn btn-danger btn-block" onClick={handleCloseDb}>
                                    ❌ Close Database
                                </button>
                            )}
                        </div>
                    </section>

                    {/* Operations Section */}
                    {dbOpen && (
                        <section className="section">
                            <h3>📝 Operations</h3>
                            <div className="input-group">
                                <input
                                    type="text"
                                    placeholder="Key"
                                    value={key}
                                    onChange={(e) => setKey(e.target.value)}
                                    onKeyDown={(e) => e.key === 'Enter' && handlePut()}
                                />
                                <input
                                    type="text"
                                    placeholder="Value (optional)"
                                    value={value}
                                    onChange={(e) => setValue(e.target.value)}
                                    onKeyDown={(e) => e.key === 'Enter' && handlePut()}
                                />
                                <div className="btn-group">
                                    <button className="btn btn-success" onClick={handlePut}>
                                        ➕ Insert
                                    </button>
                                    <button className="btn btn-danger" onClick={handleDelete}>
                                        🗑️ Delete
                                    </button>
                                </div>
                            </div>
                        </section>
                    )}

                    {/* Quick Insert Section */}
                    {dbOpen && (
                        <section className="section">
                            <h3>⚡ Quick Insert</h3>
                            <div className="quick-insert">
                                <button className="btn btn-secondary" onClick={() => handleQuickInsert(5)}>
                                    5 nums
                                </button>
                                <button className="btn btn-secondary" onClick={() => handleQuickInsert(10)}>
                                    10 nums
                                </button>
                                <button className="btn btn-secondary" onClick={() => handleQuickInsert(20)}>
                                    20 nums
                                </button>
                                <button className="btn btn-secondary" onClick={handleInsertLetters}>
                                    A-Z
                                </button>
                            </div>
                            <button className="btn btn-danger btn-block" style={{ marginTop: '0.75rem' }} onClick={handleClear}>
                                🧹 Clear All
                            </button>
                        </section>
                    )}

                    {/* Stats Section */}
                    {dbOpen && stats && (
                        <section className="section">
                            <h3>📊 Statistics</h3>
                            <div className="stats-grid">
                                <div className="stat-item">
                                    <div className="stat-value">{stats.treeHeight}</div>
                                    <div className="stat-label">Height</div>
                                </div>
                                <div className="stat-item">
                                    <div className="stat-value">{stats.pageCount}</div>
                                    <div className="stat-label">Pages</div>
                                </div>
                                <div className="stat-item">
                                    <div className="stat-value">{config.maxLeafKeys}</div>
                                    <div className="stat-label">Max Leaf</div>
                                </div>
                                <div className="stat-item">
                                    <div className="stat-value">{config.maxInteriorKeys}</div>
                                    <div className="stat-label">Max Interior</div>
                                </div>
                            </div>
                        </section>
                    )}

                    {/* Legend Section */}
                    {dbOpen && (
                        <section className="section">
                            <h3>🎨 Legend</h3>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem', fontSize: '0.875rem' }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                    <div style={{ width: '1rem', height: '1rem', borderRadius: '0.25rem', border: '2px solid #22c55e' }} />
                                    <span>Leaf Node (stores key-value pairs)</span>
                                </div>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                    <div style={{ width: '1rem', height: '1rem', borderRadius: '0.25rem', border: '2px solid #3b82f6' }} />
                                    <span>Interior Node (stores keys + child pointers)</span>
                                </div>
                            </div>
                        </section>
                    )}
                </aside>

                <div className="visualizer">
                    {!dbOpen ? (
                        <div className="empty-state">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <path d="M12 2L2 7l10 5 10-5-10-5z" />
                                <path d="M2 17l10 5 10-5" />
                                <path d="M2 12l10 5 10-5" />
                            </svg>
                            <p>Configure settings and create a database to visualize the B-tree</p>
                        </div>
                    ) : !tree ? (
                        <div className="empty-state">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <circle cx="12" cy="12" r="10" />
                                <path d="M12 6v6l4 2" />
                            </svg>
                            <p>Tree is empty. Insert some keys to see the visualization!</p>
                        </div>
                    ) : (
                        <BTreeVisualizer tree={tree} />
                    )}
                </div>
            </main>

            {toast && <div className={`toast ${toast.type}`}>{toast.message}</div>}
        </div>
    );
}

export default App;
