
import React from 'react';
import { motion } from 'framer-motion';
import { Crown, HeartPulse, Skull, Activity } from 'lucide-react';

const RaftyVisualizer = ({ state }) => {
    if (!state) return <div className="loading">Waiting for simulation data...</div>;

    const { nodes } = state;

    // Calculate positions in a pentagon (since 5 nodes)
    const radius = 220; // Increased radius
    const centerX = 300;
    const centerY = 250;

    const getNodePos = (index, total) => {
        const angle = (index * (360 / total) - 90) * (Math.PI / 180);
        return {
            x: centerX + radius * Math.cos(angle),
            y: centerY + radius * Math.sin(angle),
        };
    };

    // Draw connections first (behind nodes)
    const connections = [];
    for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
            const p1 = getNodePos(i, nodes.length);
            const p2 = getNodePos(j, nodes.length);
            connections.push(
                <line
                    key={`${i}-${j}`}
                    x1={p1.x} y1={p1.y}
                    x2={p2.x} y2={p2.y}
                    stroke="#333"
                    strokeWidth="2"
                    strokeDasharray="5,5"
                />
            );
        }
    }

    // Draw Leader-Follower lines (Active RPCs) - Visual flair
    // We don't have RPC data in state, but we can draw lines from Leader to others
    const leader = nodes.find(n => n.state === 'LEADER' && n.is_alive);
    const activeLines = [];
    if (leader) {
        const leaderIndex = nodes.findIndex(n => n.id === leader.id);
        const lp = getNodePos(leaderIndex, nodes.length);

        nodes.forEach((node, idx) => {
            if (node.id !== leader.id && node.is_alive) {
                const np = getNodePos(idx, nodes.length);
                activeLines.push(
                    <motion.line
                        key={`rpc-${leader.id}-${node.id}`}
                        x1={lp.x} y1={lp.y}
                        x2={np.x} y2={np.y}
                        stroke="#4ade80"
                        strokeWidth="3"
                        initial={{ pathLength: 0, opacity: 0 }}
                        animate={{ pathLength: [0, 1, 0], opacity: [0, 1, 0] }}
                        transition={{ duration: 0.5, repeat: Infinity, repeatDelay: 1 }}
                    />
                );
            }
        });
    }


    return (
        <div className="rafty-visualizer" style={{ width: '100%', height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
            <svg width="100%" height="100%" viewBox="0 0 600 500" preserveAspectRatio="xMidYMid meet" style={{ maxWidth: '100%', maxHeight: '100%' }}>
                {/* Network Topology */}
                <g className="connections">{connections}</g>
                <g className="active-rpc">{activeLines}</g>

                {/* Nodes */}
                {nodes.map((node, index) => {
                    const pos = getNodePos(index, nodes.length);
                    const isLeader = node.state === 'LEADER';
                    const isCandidate = node.state === 'CANDIDATE';
                    const isDead = !node.is_alive;

                    let color = '#3b82f6'; // Follower Blue
                    if (isLeader) color = '#22c55e'; // Leader Green
                    if (isCandidate) color = '#eab308'; // Candidate Yellow
                    if (isDead) color = '#ef4444'; // Dead Red

                    return (
                        <g key={node.id}>
                            {/* Halo for Leader */}
                            {isLeader && (
                                <motion.circle
                                    cx={pos.x} cy={pos.y}
                                    r={40}
                                    stroke="#22c55e"
                                    strokeWidth="2"
                                    fill="none"
                                    animate={{ r: [35, 45, 35], opacity: [0.5, 0, 0.5] }}
                                    transition={{ duration: 1.5, repeat: Infinity }}
                                />
                            )}

                            <motion.circle
                                cx={pos.x} cy={pos.y}
                                r={30}
                                fill={color}
                                stroke="#fff"
                                strokeWidth="2"
                                initial={{ scale: 0 }}
                                animate={{ scale: 1 }}
                                transition={{ type: 'spring', stiffness: 260, damping: 20 }}
                            />

                            {/* Icon */}
                            <foreignObject x={pos.x - 12} y={pos.y - 12} width="24" height="24">
                                <div style={{ color: 'white', display: 'flex', justifyContent: 'center' }}>
                                    {isDead ? <Skull size={24} /> :
                                        isLeader ? <Crown size={24} /> :
                                            isCandidate ? <Activity size={24} /> :
                                                <HeartPulse size={24} />}
                                </div>
                            </foreignObject>

                            {/* Labels */}
                            <text x={pos.x} y={pos.y + 45} textAnchor="middle" fill="#fff" fontSize="14" fontWeight="bold">Node {node.id}</text>
                            <text x={pos.x} y={pos.y + 60} textAnchor="middle" fill="#aaa" fontSize="10">{node.state}</text>
                            <text x={pos.x} y={pos.y + 72} textAnchor="middle" fill="#666" fontSize="10">Term: {node.current_term}</text>
                        </g>
                    );
                })}
            </svg>
        </div>
    );
};

export default RaftyVisualizer;
