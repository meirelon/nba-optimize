**_PLEASE put all exploratory work in the notebooks directory_**

## NBA Player Prediction and Optimization
Pipeline for NBA Optimization

## Dry Run
```
python dk_prod/optimize.py --dk_link="https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=70&draftGroupId=22401"
```

## Run flask
```
curl -X POST -d '{"dk_url":"https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=70&draftGroupId=22799"}' https://scarlet-labs.appspot.com/optimize -H "Content-Type: application/json"
```
