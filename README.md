Amazon Reviews Database Project

This project designs and implements a complete data management and visualization system for Amazon product reviews using MySQL, MongoDB, and Neo4j.
It combines relational, non-relational, and graph databases with an interactive Dash dashboard for analysis and visualization.

Authors

Javier Mendoza Guerrero

Luis Bertrán Vidal Campos

Main Features

MySQL: structured storage (users, products, reviews).

MongoDB: unstructured text storage (reviewText, summary, helpful).

Neo4j: graph relationships between users and products.

Dash Dashboard: interactive visual analysis (temporal trends, popularity, word clouds).

Key Scripts

load_data.py → creates databases and loads datasets.

inserta_dataset.py → adds an extra dataset (Pet_Supplies_5.json).

menu_visualizacion.py → launches the Dash dashboard.

neo4JProyecto.py → executes graph-based analysis in Neo4j.

configuracion.py → connection settings for MySQL and MongoDB.

How to Run

Start MySQL, MongoDB, and Neo4j.

Edit your credentials in configuracion.py.

Run:

python load_data.py
python menu_visualizacion.py


Open http://127.0.0.1:8050/
 to access the dashboard.

Optionally:

python neo4JProyecto.py

Technologies

Python · MySQL · MongoDB · Neo4j · Dash · Plotly · Matplotlib · WordCloud

License

MIT License