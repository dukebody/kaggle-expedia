import pandas as pd
import ml_metrics


real = pd.read_csv('t2.csv')
predictions = pd.read_csv('submission_last.csv')

# predictions are in format " c1 c2 c3 c4 c5"
clusters_pred = predictions['hotel_cluster'].str.strip().str.split(' ')
# need to cast to str bc predictions are str as well
clusters_real = [[str(l)] for l in real["hotel_cluster"]]

print(ml_metrics.mapk(clusters_real, clusters_pred, k=5))
