import pandas as pd
data = [1,2,3,4,5]
df = pd.DataFrame(data)
df.to_gbq(project_id='scarlet-labs', destination_table='testing.iris_test', if_exists="replace")
