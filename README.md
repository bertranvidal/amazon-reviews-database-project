Amazon Reviews Database Project

This project designs and implements a complete data management and visualization system for Amazon product reviews using MySQL, MongoDB, and Neo4j.
It combines relational, non-relational, and graph databases with an interactive Dash dashboard for analysis and visualization.

👥 Authors

Javier Mendoza Guerrero

Luis Bertrán Vidal Campos

⚙️ Main Features

MySQL: structured storage (users, products, reviews).

MongoDB: unstructured text storage (reviewText, summary, helpful).

Neo4j: graph relationships between users and products.

Dash Dashboard: interactive visual analysis (temporal trends, popularity, word clouds).

📁 Key Scripts
File	Description
load_data.py	Creates databases and loads datasets.
inserta_dataset.py	Adds dataset
menu_visualizacion.py	Launches the Dash dashboard.
neo4JProyecto.py	Runs graph-based analysis in Neo4j.
configuracion.py	Connection settings for MySQL and MongoDB.
🚀 How to Run

Start MySQL, MongoDB, and Neo4j.

Edit your credentials in configuracion.py.

Execute:

python load_data.py
python menu_visualizacion.py


Open http://127.0.0.1:8050/

(Optional) Run graph analysis:

python neo4JProyecto.py

🧰 Technologies

Python, MySQL, MongoDB, Neo4j, Dash, Plotly, Matplotlib, WordCloud

📜 License

MIT License
