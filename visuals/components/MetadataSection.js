import 'reactflow/dist/style.css';
import ListItemButton from '@mui/material/ListItemButton'
import ListItemText from '@mui/material/ListItemText'
import List from '@mui/material/List'
import '@fontsource/roboto/400.css';


function MetadataSection(reactFlowStyle){
    return (<div style={reactFlowStyle}> 
        <h1 style={{fontFamily: "roboto"}}>Dependencies</h1>
        <List>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        </List>
        
  
        <h1>Downstream tasks</h1>
        <List>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        <ListItemButton component="a" href="#simple-list">
          <ListItemText primary="Downstream Task 1" />
        </ListItemButton>
        </List>
      </div>)
}

export default MetadataSection;