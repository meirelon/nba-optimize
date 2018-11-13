from datetime import datetime, timedelta
import logging
from flask import Flask, request
from dk_prod import optimize, player, FeatureBuilding, prod_utils

app = Flask(__name__)

# @app.route('/nbadk/optimize')

@app.route('/test', methods=['GET', 'POST'])
def test():
    input = request.get_json()
    # this is the draftkings link (str)
    dk_url = input.get('dk_url')
    if dk_url is None:
        return "No url found", 400

    total_lineups = input.get('total_lineups', 50)
    optimize_pipeline = optimize.DraftKingsNBAOptimizeLineups(project='scarlet-labs',
													dataset='draftkings',
													season='2019',
													partition_date=None,
													dk_link=dk_url,
													total_lineups=int(total_lineups))
    return optimize_pipeline.optimize()

@app.errorhandler(500) #error handling script for troubleshooting
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__': #hosting administration syntax
    app.run(host='127.0.0.1', port=8080, debug=True)
