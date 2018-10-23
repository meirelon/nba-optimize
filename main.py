from datetime import datetime, timedelta
import logging
from flask import Flask, request
from scrape_foos import scrape

app = Flask(__name__)

@app.route('/bbref-scrape/get-nba-data') #make up memorable URL-will be used in cron job syntax
def start_get_data(): #make up memorable function name for cron job
    is_cron = request.headers.get('X-Appengine-Cron', False)
    if not is_cron:
        return 'Bad Request', 400

    try:
        year = '2019'
        sport_type='basketball'
        project = 'scarlet-labs'
        url = 'https://www.basketball-reference.com/leagues/NBA_YYYY_per_game.html'.replace('YYYYMMDD', year)
        scraper = scrape.bbref_scrape(year=year, sport_type=sport_type, url=url, project=project)
        scraper.run() #the actual name of the script/function you want to run contained in the subfolder
        return "Pipeline started", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.errorhandler(500) #error handling script for troubleshooting
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__': #hosting administration syntax
    app.run(host='127.0.0.1', port=8080, debug=True)
