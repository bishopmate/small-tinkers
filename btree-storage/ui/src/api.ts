const API_BASE = '/api';

export interface TreeNode {
    pageId: number;
    isLeaf: boolean;
    keys: string[];
    values: string[];
    children: TreeNode[];
}

export interface BTreeConfig {
    maxLeafKeys: number;
    maxInteriorKeys: number;
}

export interface Stats {
    pageCount: number;
    bufferPoolSize: number;
    treeHeight: number;
}

export interface StatsWithConfig extends Stats {
    btreeConfig: BTreeConfig;
}

export interface TreeResponse {
    tree: TreeNode | null;
    stats: StatsWithConfig | null;
}

export interface OperationResponse {
    success: boolean;
    message: string;
}

export interface GetResponse {
    key: string;
    value: string | null;
    found: boolean;
}

async function handleResponse<T>(response: Response): Promise<T> {
    if (!response.ok) {
        const error = await response.json().catch(() => ({ message: 'Unknown error' }));
        throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
}

export const api = {
    async createDb(config: { maxLeafKeys?: number; maxInteriorKeys?: number; path?: string }) {
        const response = await fetch(`${API_BASE}/db`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
        });
        return handleResponse<OperationResponse>(response);
    },

    async closeDb() {
        const response = await fetch(`${API_BASE}/db`, { method: 'DELETE' });
        return handleResponse<OperationResponse>(response);
    },

    async getConfig() {
        const response = await fetch(`${API_BASE}/config`);
        return handleResponse<BTreeConfig>(response);
    },

    async setConfig(config: Partial<BTreeConfig>) {
        const response = await fetch(`${API_BASE}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
        });
        return handleResponse<OperationResponse>(response);
    },

    async get(key: string) {
        const response = await fetch(`${API_BASE}/kv/${encodeURIComponent(key)}`);
        return handleResponse<GetResponse>(response);
    },

    async put(key: string, value: string) {
        const response = await fetch(`${API_BASE}/kv`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key, value }),
        });
        return handleResponse<OperationResponse>(response);
    },

    async delete(key: string) {
        const response = await fetch(`${API_BASE}/kv/${encodeURIComponent(key)}`, {
            method: 'DELETE',
        });
        return handleResponse<OperationResponse>(response);
    },

    async listKeys() {
        const response = await fetch(`${API_BASE}/keys`);
        return handleResponse<string[]>(response);
    },

    async getTree() {
        const response = await fetch(`${API_BASE}/tree`);
        return handleResponse<TreeResponse>(response);
    },

    async getStats() {
        const response = await fetch(`${API_BASE}/stats`);
        return handleResponse<StatsWithConfig>(response);
    },

    async clear() {
        const response = await fetch(`${API_BASE}/clear`, { method: 'POST' });
        return handleResponse<OperationResponse>(response);
    },

    async bulkInsert(pairs: Array<{ key: string; value: string }>) {
        const response = await fetch(`${API_BASE}/bulk`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pairs }),
        });
        return handleResponse<OperationResponse>(response);
    },
};
