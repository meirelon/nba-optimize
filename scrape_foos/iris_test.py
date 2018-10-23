from sklearn.datasets import load_iris
import pandas as pd

df = load_iris()
df.to_gbq(project='scarlet-labs', destination_table='testing.iris_test', if_exists="replace")
