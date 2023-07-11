from flask import Flask, render_template
from serra.config import TEMPLATE_FOLDER


EXAMPLE_NODES_LIST = [
        { "id": 1, "label": "Node 1" },
        { "id": 2, "label": "Node 2" },
        { "id": 3, "label": "Node 3" },
        { "id": 4, "label": "Node 4" },
        { "id": 5, "label": "Node 5" },
      ]

EXAMPLE_EDGES_LIST = [
        { "from": 1, "to": 3 },
        { "from": 1, "to": 2 },
        { "from": 2, "to": 4 },
        { "from": 2, "to": 5 }
      ]

def start_server(job_steps):
    app = Flask(__name__, 
                template_folder=TEMPLATE_FOLDER)
    
    nodes_list = [{"id": job, "label": job} for job in job_steps]
    edges_list = []
    for i in range(len(job_steps)):
        if i == len(job_steps) - 1:
            break
        edge = {"from": job_steps[i], "to": job_steps[i+1]}
        edges_list.append(edge)

    @app.route('/')
    def serve_index():
        return render_template('index.html', nodes_list=nodes_list, edges_list=edges_list)

    app.run()

# if __name__ == '__main__':
#     start_server()