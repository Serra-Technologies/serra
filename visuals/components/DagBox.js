import React from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  MarkerType,
  useNodesState, 
  useEdgesState
} from 'reactflow';

import 'reactflow/dist/style.css';
import '@fontsource/roboto/400.css';

const reactFlowStyle = {
    width: '50vw', 
    height: '90vh',
    // display: "flex"
  }


function DagBox(){

    const initialNodes = [
        { id: 'sales', position: { x: 250, y: 100 }, data: { label: 'Sales Table' },type: 'input' },
        { id: 'step_read', position: { x: 250, y: 200 }, data: { label: 'Name: step_read \n Type: MapTransformer' } }
    ];
    const initialEdges = [{ id: 'e1-2', 
                            source: 'sales', 
                            target: 'step_read',
                            markerEnd: {
                                type: MarkerType.ArrowClosed,
                            },}];

    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);


    return (<div style={reactFlowStyle}>
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
    >
      <Controls />
      <MiniMap />
      <Background variant="dots" gap={12} size={1} />
    </ReactFlow>
    
  </div>)
}


export default DagBox;

