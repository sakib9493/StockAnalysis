"""
Implementation of Modern Portfolio Theory
"""

import numpy as np
import datetime as dt
import pandas as pd
import scipy.optimize as sc
from pandas_datareader import data as pdr
import plotly.graph_objects as go

#Import Data
def get_data(stocks, start, end):
    stock_data = pdr.get_data_yahoo(stocks, start=start, end=end)
    stock_data = stock_data['Adj Close']
    returns = stock_data.pct_change()
    #returns = np.log(stock_data).diff()
    mean_returns = returns.mean()
    cov_matrix = returns.cov()
    return mean_returns, cov_matrix

def portfolio_performance(weights, mean_returns, cov_matrix):
    returns = np.sum(mean_returns * weights) * 252
    std = np.sqrt(weights.T @ cov_matrix @ weights) * np.sqrt(252)
    return returns, std

def negative_sharpe_ratio(weights, mean_returns, cov_matrix, risk_free_rate=0):
    returns, std = portfolio_performance(weights, mean_returns, cov_matrix)
    return - (returns - risk_free_rate) / std

def max_sharpe_ratio(mean_returns, cov_matrix, risk_free_rate=0, constraint_set=(0, 1)):
    "Minimize the negative sharpe ratio by altering the weights of the portfolio"
    num_assets = len(mean_returns)
    args = (mean_returns, cov_matrix, risk_free_rate)
    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    bound = constraint_set
    bounds = tuple(bound for asset in range(num_assets))
    result = sc.minimize(negative_sharpe_ratio, num_assets * [1./num_assets],
                                  args=args,method='SLSQP', bounds=bounds,
                                  constraints=constraints)
    return result

def portfolio_variance(weights, mean_returns, cov_matrix):
    return portfolio_performance(weights, mean_returns, cov_matrix)[1]

def minimize_variance(mean_returns, cov_matrix, constraint_set=(0, 1)):
    "Minimize the portfolio variance by altering the weights/allocation of assets in the portfolio"
    num_assets = len(mean_returns)
    args = (mean_returns, cov_matrix)
    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    bound = constraint_set
    bounds = tuple(bound for asset in range(num_assets))
    result = sc.minimize(portfolio_variance, num_assets * [1. / num_assets],
                                  args=args, method='SLSQP', bounds=bounds,
                                  constraints=constraints)
    return result

def portfolio_return(weights, mean_returns, cov_matrix):
    return portfolio_performance(weights, mean_returns, cov_matrix)[0]

def efficient_optimization(mean_returns, cov_matrix, return_target, constraint_set=(0, 1)):
    """
    For each return_target, we want to optimize the portfolio for min variance
    """
    num_assets = len(mean_returns)
    args = (mean_returns, cov_matrix)

    constraints = ({'type': 'eq', 'fun': lambda x: portfolio_return(x, mean_returns, cov_matrix) - return_target},
                   {'type': 'eq', 'fun': lambda x: np.sum(x) - 1})

    bound = constraint_set
    bounds = tuple(bound for asset in range(num_assets))
    effOpt = sc.minimize(portfolio_variance, num_assets * [1. / num_assets], args=args,
                         method='SLSQP', bounds=bounds, constraints=constraints)
    return effOpt

def calculated_results(mean_returns, cov_matrix, risk_free_rate=0, constraint_set=(0, 1)):
    """
    Read in mean, cov matrix and other financial information
    Output: Max Sharpe Ratio, Min Volatility, Efficient Frontier
    """
    # Max Sharpe Ratio Portfolio
    max_SR_portfolio = max_sharpe_ratio(mean_returns, cov_matrix)
    max_SR_returns, max_SR_std = portfolio_performance(max_SR_portfolio['x'],
                                                       mean_returns, cov_matrix)
    max_SR_allocation = pd.DataFrame(max_SR_portfolio['x'], index=mean_returns.index,
                                     columns=['allocation'])
    max_SR_allocation.allocation = [round(i * 100, 0) for i in max_SR_allocation.allocation]

    # Min Volatility Portfolio
    min_Vol_portfolio = minimize_variance(mean_returns, cov_matrix)
    min_Vol_returns, min_Vol_std = portfolio_performance(min_Vol_portfolio['x'],
                                                       mean_returns, cov_matrix)
    min_Vol_allocation = pd.DataFrame(min_Vol_portfolio['x'], index=mean_returns.index,
                                     columns=['allocation'])
    min_Vol_allocation.allocation = [round(i * 100, 0) for i in min_Vol_allocation.allocation]

    # Efficient Frontier
    efficient_list = []
    target_returns = np.linspace(min_Vol_returns, max_SR_returns, 20)
    for target in target_returns:
        efficient_list.append(efficient_optimization(mean_returns, cov_matrix, target)['fun'])

    max_SR_returns, max_SR_std = round(max_SR_returns * 100, 2), round(max_SR_std * 100, 2)
    min_Vol_returns, min_Vol_std = round(min_Vol_returns * 100, 2), round(min_Vol_std * 100, 2)


    return max_SR_returns, max_SR_std, max_SR_allocation, min_Vol_returns, min_Vol_std, min_Vol_allocation, efficient_list, target_returns

def EF_graph(mean_returns, cov_matrix, risk_free_rate=0, constraint_set=(0,1)):
    """
    Returns a graph ploting the min volatility, max sharpe ratio and efficient frontier
    """
    max_SR_returns, max_SR_std, max_SR_allocation, min_Vol_returns, min_Vol_std, min_Vol_allocation, efficient_list, target_returns = calculated_results(mean_returns, cov_matrix, risk_free_rate, constraint_set)

    # Max SR
    Max_Sharpe_Ratio = go.Scatter(
        name='Maximum Sharpe Ratio',
        mode='markers',
        x=[max_SR_std],
        y=[max_SR_returns],
        marker=dict(color='red',size=14,line=dict(width=3, color='black'))
    )

    # Min Vol
    Min_Vol = go.Scatter(
        name='Minimum Volatility',
        mode='markers',
        x=[min_Vol_std],
        y=[min_Vol_returns],
        marker=dict(color='green', size=14, line=dict(width=3, color='black'))
    )

    # Efficient Frontier
    EF_curve = go.Scatter(
        name='Efficient Frontier',
        mode='lines',
        x=[round(ef_std * 100, 2) for ef_std in efficient_list],
        y=[round(target * 100, 2) for target in target_returns],
        line=dict(color='black', width=4, dash='dashdot')
    )

    data = [Max_Sharpe_Ratio, Min_Vol, EF_curve]

    layout = go.Layout(
        title='Porfolio Optimization with Efficient Frontier',
        yaxis=dict(title='Annualized Return (%)'),
        xaxis=dict(title='Annualized Volatility (%)'),
        showlegend=True,
        legend=dict(
            x=0.75, y=0, traceorder='normal',
            bgcolor='#E2E2E2',
            bordercolor='black',
            borderwidth=2),
        width=800,
        height=600)

    fig = go.Figure(data=data, layout=layout)
    return fig.show()
stock_list = ['AAPL', 'GOOG', 'NVDA']

end_date = dt.datetime.now()
start_date = end_date - dt.timedelta(days=365)

mean_returns, cov_matrix = get_data(stock_list, start_date, end_date)

#print(calculated_results(mean_returns, cov_matrix))

EF_graph(mean_returns, cov_matrix)

