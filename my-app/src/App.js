import React, { useState, useEffect } from "react";
import "./App.css";

function App() {
  const [map, setMap] = useState("");

  useEffect(() => {
    async function fetchMap() {
      const response = await fetch("http://localhost:8000/generateMap");
      const mapObject = await response.json();
      setMap(`data:image/png;base64,${mapObject.img}`);
    }

    fetchMap();
  }, []);

  return <div style={{ textAlign: "center" }}>{map && <img src={map} />}</div>;
}

export default App;
