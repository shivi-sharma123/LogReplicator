
import React from 'react';

const ReplicatedLog = ({ nodes }) => {
    return (
        <div className="replicated-log panel">
            <h3>📓 Replicated Log</h3>
            <div className="log-grid">
                {nodes.map(node => (
                    <div key={node.id} className={`log-row ${!node.is_alive ? 'dead' : ''}`}>
                        <div className="node-id-badge">N{node.id}</div>
                        <div className="entries">
                            {/* Placeholder for when we have real log entries */}
                            {node.log && node.log.length > 0 ? (
                                node.log.map((entry, i) => (
                                    <div key={i} className="log-entry-block">
                                        <span className="term">{entry.term}</span>
                                    </div>
                                ))
                            ) : (
                                <span className="empty-log">Empty Log</span>
                            )}

                            {/* Visual filler for empty space if log is short */}
                            <div className="log-track-line"></div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default ReplicatedLog;
