import React, { useState, useEffect } from "react";
import "./App.css";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";

function App() {
  const [map, setMap] = useState("");
  const [request, setRequest] = useState("");

  useEffect(() => {
    async function fetchMap() {
      console.log(request);
      const response = await fetch(
        request || "http://localhost:8000/earthquakeMap"
      );
      const mapObject = await response.json();
      setMap(`data:image/png;base64,${mapObject.img}`);
    }

    fetchMap();

    const refreshInterval = setInterval(fetchMap, 30000);

    return () => clearInterval(refreshInterval);
  }, [request]);

  async function earthquakeMap() {
    setRequest("http://localhost:8000/earthquakeMap");
    const response = await fetch("http://localhost:8000/earthquakeMap");
    const mapObject = await response.json();
    setMap(`data:image/png;base64,${mapObject.img}`);
  }

  async function wildfireMap() {
    setRequest("http://localhost:8000/wildfireMap");
    const response = await fetch("http://localhost:8000/wildfireMap");
    const mapObject = await response.json();
    setMap(`data:image/png;base64,${mapObject.img}`);
  }

  return (
    <div>
      <ButtonGroup style={{ textAlign: "center" }}>
        <Button
          style={{ fontSize: "20px", padding: "10px 20px" }}
          onClick={earthquakeMap}
        >
          Earthquake
        </Button>
        <Button
          style={{ fontSize: "20px", padding: "10px 20px" }}
          onClick={wildfireMap}
        >
          Wildfire
        </Button>
      </ButtonGroup>
      <div style={{ textAlign: "center" }}>
        {map && <img src={map} style={{ width: "80%", height: "auto" }} />}
      </div>
    </div>
  );
}

export default App;
