# Notebooks Directory

This directory contains Jupyter notebooks for data exploration and analysis.

## Available Notebooks

### 📈 `solana_data_explorer.ipynb`
Interactive exploration of Solana blockchain data:
- Token whale analysis
- Wallet transaction patterns
- PnL performance metrics
- Data quality assessment

### 🏆 `top_traders_explorer.ipynb`
Top trader identification and analysis:
- Elite trader performance metrics
- Portfolio analytics
- Trading strategy insights
- Performance tier classification

## Usage

These notebooks are designed for:
- **Data Exploration**: Interactive analysis of pipeline data
- **Prototyping**: Testing new analysis approaches
- **Visualization**: Creating charts and graphs for insights
- **Documentation**: Demonstrating pipeline capabilities

## Requirements

To run these notebooks:
1. Ensure the Airflow environment is running
2. Access MinIO data storage at `http://localhost:9000`
3. Connect to DuckDB analytics database
4. Install Jupyter dependencies in the project environment

## Development Workflow

1. **Exploration**: Use notebooks to explore new data sources
2. **Prototyping**: Test analysis logic before implementing in DAGs
3. **Validation**: Verify pipeline outputs and data quality
4. **Documentation**: Create visual summaries of findings

These notebooks complement the production pipeline by providing an interactive environment for data scientists and analysts.