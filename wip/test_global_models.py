import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Dados de teste (2024)
test_data = df[df['date'] >= '2024-01-01']

# Coeficientes do modelo
k0, k1, k2, k3, k4 = model.params  # Substituir pelos coeficientes reais

# Cálculo de z
test_data['z'] = (k0 +
                  k1 * test_data['1/T'] +
                  k2 * test_data['x'] +
                  k3 * test_data['iv'] +
                  k4 * test_data['F'])

# Cálculo de y estimado
test_data['y_est'] = ((1 + test_data['x']**test_data['z'])**(1 / test_data['z'])) - 1

# Cálculo de C estimado
test_data['C_est'] = test_data['y_est'] * test_data['S']

# Avaliação do modelo
mae = mean_absolute_error(test_data['C_observed'], test_data['C_est'])
rmse = np.sqrt(mean_squared_error(test_data['C_observed'], test_data['C_est']))
mape = np.mean(np.abs((test_data['C_observed'] - test_data['C_est']) / test_data['C_observed'])) * 100
r2 = r2_score(test_data['C_observed'], test_data['C_est'])

print(f"MAE: {mae:.4f}, RMSE: {rmse:.4f}, MAPE: {mape:.2f}%, R^2: {r2:.4f}")

# Análise residual
test_data['residual'] = test_data['C_observed'] - test_data['C_est']
test_data['residual'].plot(kind='hist', bins=50, title='Distribuição dos Resíduos')