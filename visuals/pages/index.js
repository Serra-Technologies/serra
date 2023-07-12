import React, { useCallback ,useState} from 'react';

import 'reactflow/dist/style.css';
import '@fontsource/roboto/400.css';
import ButtonAppBar from '../components/ButtonAppBar'
import DagBox from '../components/DagBox'
import MetadataSection from '../components/MetadataSection';





export default function App() {
  

  const reactFlowStyle = {
    width: '50vw', 
    height: '80vh',
    // display: "flex"
  }

  const two_sections = {
    display:"flex"
  }

  const flex_parent = {
    "display":"flex", 
    "flexDirection": "row",
    fontFamily: "roboto"
  }

  return (
    
    <div>
      <ButtonAppBar></ButtonAppBar>
    
    <div style={flex_parent}>
    <DagBox></DagBox>
    <MetadataSection reactFlowStyle={reactFlowStyle}></MetadataSection>
    </div>
    </div>
  );
}