import React, { useCallback ,useState} from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  MarkerType,
  useNodesState,
  useEdgesState,
  addEdge,
} from 'reactflow';

import 'reactflow/dist/style.css';


const initialNodes = [
  { id: '1', position: { x: 500, y: 100 }, data: { label: '1' },type: 'input' },
  { id: '2', position: { x: 500, y: 200 }, data: { label: '2' } }
];
const initialEdges = [{ id: 'e1-2', 
                        source: '1', 
                        target: '2',
                        markerEnd: {
                          type: MarkerType.ArrowClosed,
                        },}];



export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  return (
    <div style={{ width: '100vw', height: '100vh', border: "3px"}}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
      />
    </div>
  );
}