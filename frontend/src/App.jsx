
import React, { useState, useEffect, useRef } from 'react';
import RaftyVisualizer from './components/RaftyVisualizer';
import ControlPanel from './components/ControlPanel';
import LogStream from './components/LogStream';
import ReplicatedLog from './components/ReplicatedLog';
import axios from 'axios';
import './App.css';

const API_URL = 'http://localhost:8080';
const WS_URL = 'ws://localhost:8080/ws';

function App() {
  const [clusterState, setClusterState] = useState(null);
  const [logs, setLogs] = useState([]);
  const wsRef = useRef(null);

  const connectWebSocket = () => {
    wsRef.current = new WebSocket(WS_URL);

    wsRef.current.onopen = () => {
      console.log('Connected to WebSocket');
      addLog('System', 'Connected to simulation backend');
    };

    wsRef.current.onmessage = (event) => {
      const state = JSON.parse(event.data);
      setClusterState(state);
    };

    wsRef.current.onclose = () => {
      console.log('Disconnected from WebSocket');
      addLog('System', 'Disconnected from backend. Retrying in 2s...');
      setTimeout(connectWebSocket, 2000);
    };
  };

  useEffect(() => {
    connectWebSocket();

    return () => {
      if (wsRef.current) {
        wsRef.current.onclose = null; // Prevent retry on unmount
        wsRef.current.close();
      }
    };
  }, []);

  const addLog = (source, message) => {
    setLogs(prev => [{
      id: Date.now() + Math.random(),
      time: new Date().toLocaleTimeString(),
      source,
      message
    }, ...prev].slice(0, 50));
  };

  // Diffing for logs
  const prevStateRef = useRef(null);
  useEffect(() => {
    if (!clusterState || !prevStateRef.current) {
      prevStateRef.current = clusterState;
      return;
    }

    // Check for leader changes
    const prevLeader = prevStateRef.current.nodes.find(n => n.state === 'LEADER');
    const currLeader = clusterState.nodes.find(n => n.state === 'LEADER');

    if (currLeader && (!prevLeader || prevLeader.id !== currLeader.id)) {
      addLog('Election', `Node ${currLeader.id} elected Leader for Term ${currLeader.current_term}`);
    }

    // Check for state changes
    clusterState.nodes.forEach(node => {
      const prevNode = prevStateRef.current.nodes.find(n => n.id === node.id);
      if (!prevNode) return;

      if (node.current_term > prevNode.current_term) {
        addLog(`Node ${node.id}`, `Term increased to ${node.current_term}`);
      }
      if (node.state !== prevNode.state) {
        // addLog(`Node ${node.id}`, `State changed to ${node.state}`);
      }
    });

    prevStateRef.current = clusterState;
  }, [clusterState]);

  const handleStopNode = async (id) => {
    try {
      await axios.post(`${API_URL}/control/stop/${id}`);
      addLog('User', `Stopped Node ${id}`);
    } catch (err) {
      console.error(err);
    }
  };

  const handleStartNode = async (id) => {
    try {
      await axios.post(`${API_URL}/control/start/${id}`);
      addLog('User', `Started Node ${id}`);
    } catch (err) {
      console.error(err);
    }
  };

  const handleSubmitWork = async () => {
    try {
      const cmd = `CMD-${Math.floor(Math.random() * 1000)}`;
      await axios.post(`${API_URL}/control/submit`, { command: cmd });
      addLog('User', `Submitted command: ${cmd}`);
    } catch (err) {
      addLog('Error', 'Failed to submit work (No Leader?)');
    }
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>🚣 Raft Consensus Simulator</h1>
        <div className="status-badge">
          {clusterState ? <span className="online">● System Online</span> : <span className="offline">● Connecting...</span>}
        </div>
      </header>

      <div className="main-content">
        <div className="visualization-area">
          <RaftyVisualizer state={clusterState} />
        </div>

        <div className="middle-panel">
          <ReplicatedLog nodes={clusterState?.nodes || []} />
        </div>

        <div className="sidebar">
          <ControlPanel
            nodes={clusterState?.nodes || []}
            onStop={handleStopNode}
            onStart={handleStartNode}
            onSubmit={handleSubmitWork}
          />
          <LogStream logs={logs} />
        </div>
      </div>
    </div>
  );
}

export default App;
