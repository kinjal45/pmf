from flask import Flask, request, make_response

app = Flask(__name__)


@app.route('/predictions', methods=['POST'])
def predictions():
    import subprocess
    subprocess.call(['/run_job.sh'], shell=True)
    return make_response(200)


@app.route('/model', methods=['POST'])
def model():
    print(request.json)
    import subprocess
    subprocess.call(['/build_model.sh'], shell=True)
    return make_response(200)


if __name__ == '__main__':
    app.run(port=5000)

