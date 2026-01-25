import { useMemo } from 'react';
import { TreeNode } from '../api';

interface Props {
    tree: TreeNode;
}

interface NodePosition {
    node: TreeNode;
    x: number;
    y: number;
    width: number;
}

interface LayoutResult {
    nodes: NodePosition[];
    edges: Array<{ from: NodePosition; to: NodePosition }>;
    width: number;
    height: number;
}

const NODE_HEIGHT = 50;
const NODE_MIN_WIDTH = 80;
const KEY_WIDTH = 30;
const LEVEL_GAP = 80;
const NODE_GAP = 20;

function calculateNodeWidth(node: TreeNode): number {
    const keysWidth = node.keys.length * KEY_WIDTH + (node.keys.length - 1) * 4 + 20;
    return Math.max(NODE_MIN_WIDTH, keysWidth);
}

function layoutTree(root: TreeNode): LayoutResult {
    const nodes: NodePosition[] = [];
    const edges: Array<{ from: NodePosition; to: NodePosition }> = [];

    // First pass: calculate subtree widths
    function getSubtreeWidth(node: TreeNode): number {
        if (node.children.length === 0) {
            return calculateNodeWidth(node);
        }
        const childrenWidth = node.children.reduce(
            (sum, child) => sum + getSubtreeWidth(child) + NODE_GAP,
            -NODE_GAP
        );
        return Math.max(calculateNodeWidth(node), childrenWidth);
    }

    // Second pass: position nodes
    function positionNode(node: TreeNode, x: number, y: number, availableWidth: number): NodePosition {
        const nodeWidth = calculateNodeWidth(node);
        const nodeX = x + availableWidth / 2;
        const pos: NodePosition = { node, x: nodeX, y, width: nodeWidth };
        nodes.push(pos);

        if (node.children.length > 0) {
            const totalChildrenWidth = node.children.reduce(
                (sum, child) => sum + getSubtreeWidth(child) + NODE_GAP,
                -NODE_GAP
            );
            let childX = x + (availableWidth - totalChildrenWidth) / 2;

            for (const child of node.children) {
                const childWidth = getSubtreeWidth(child);
                const childPos = positionNode(child, childX, y + LEVEL_GAP + NODE_HEIGHT, childWidth);
                edges.push({ from: pos, to: childPos });
                childX += childWidth + NODE_GAP;
            }
        }

        return pos;
    }

    const totalWidth = getSubtreeWidth(root);
    positionNode(root, 0, 20, totalWidth);

    // Calculate dimensions
    const maxY = Math.max(...nodes.map((n) => n.y));
    const height = maxY + NODE_HEIGHT + 40;

    return { nodes, edges, width: totalWidth + 40, height };
}

export function BTreeVisualizer({ tree }: Props) {
    const layout = useMemo(() => layoutTree(tree), [tree]);

    return (
        <svg
            className="tree-svg"
            viewBox={`0 0 ${layout.width} ${layout.height}`}
            style={{ maxWidth: '100%', height: 'auto', minHeight: '400px' }}
        >
            {/* Draw edges first (behind nodes) */}
            {layout.edges.map((edge, i) => (
                <path
                    key={`edge-${i}`}
                    className="edge"
                    d={`M ${edge.from.x} ${edge.from.y + NODE_HEIGHT / 2} 
              C ${edge.from.x} ${edge.from.y + NODE_HEIGHT / 2 + 30},
                ${edge.to.x} ${edge.to.y - 30},
                ${edge.to.x} ${edge.to.y - NODE_HEIGHT / 2 + 5}`}
                />
            ))}

            {/* Draw nodes */}
            {layout.nodes.map((pos, i) => (
                <g key={`node-${i}`} className="node-group" transform={`translate(${pos.x}, ${pos.y})`}>
                    {/* Node rectangle */}
                    <rect
                        className={`node-rect ${pos.node.isLeaf ? 'leaf' : 'interior'}`}
                        x={-pos.width / 2}
                        y={-NODE_HEIGHT / 2}
                        width={pos.width}
                        height={NODE_HEIGHT}
                        rx={6}
                    />

                    {/* Page ID label */}
                    <text className="node-label" x={-pos.width / 2 + 6} y={-NODE_HEIGHT / 2 + 12}>
                        P{pos.node.pageId}
                    </text>

                    {/* Keys */}
                    {pos.node.keys.map((key, keyIndex) => {
                        const keyOffset = (keyIndex - (pos.node.keys.length - 1) / 2) * (KEY_WIDTH + 4);
                        return (
                            <g key={`key-${keyIndex}`}>
                                <rect
                                    x={keyOffset - KEY_WIDTH / 2}
                                    y={-8}
                                    width={KEY_WIDTH}
                                    height={20}
                                    rx={4}
                                    fill="#334155"
                                />
                                <text className="node-text" x={keyOffset} y={7} textAnchor="middle">
                                    {key.length > 4 ? key.slice(0, 3) + '…' : key}
                                </text>
                            </g>
                        );
                    })}

                    {/* Values for leaf nodes (shown below keys) */}
                    {pos.node.isLeaf && pos.node.values.length > 0 && (
                        <text className="node-label" x={0} y={NODE_HEIGHT / 2 - 4} textAnchor="middle">
                            {pos.node.values.length <= 3
                                ? pos.node.values.map((v) => (v.length > 3 ? v.slice(0, 2) + '…' : v)).join(', ')
                                : `${pos.node.values.length} values`}
                        </text>
                    )}
                </g>
            ))}
        </svg>
    );
}
