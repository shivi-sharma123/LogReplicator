
import React, { useEffect, useRef } from 'react';

const LogStream = ({ logs }) => {
    const bottomRef = useRef(null);

    useEffect(() => {
        bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [logs]);

    return (
        <div className="log-stream panel">
            <h3>📜 Event Log</h3>
            <div className="log-container">
                {logs.length === 0 && <div className="log-empty">No events yet...</div>}
                {logs.map(log => (
                    <div key={log.id} className="log-entry">
                        <span className="log-time">{log.time}</span>
                        <span className="log-source">[{log.source}]</span>
                        <span className="log-message">{log.message}</span>
                    </div>
                ))}
                <div ref={bottomRef} />
            </div>
        </div>
    );
};

export default LogStream;
