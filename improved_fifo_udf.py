"""
Improved FIFO UDF for Silver Wallet PnL Calculation
Handles edge cases like SELL-before-BUY sequences and partial matching
"""

def calculate_improved_token_pnl_fifo(transactions):
    """
    Improved FIFO PnL calculation that handles:
    1. SELL transactions before any BUY (tracks as negative inventory)
    2. Partial matching scenarios
    3. Better price/value handling
    4. More accurate trade counting
    """
    if not transactions:
        return (0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
    
    # Convert to list and sort by timestamp
    txn_list = []
    for row in transactions:
        txn_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
        txn_list.append(txn_dict)
    
    txn_list.sort(key=lambda x: x['timestamp'])
    
    # Enhanced FIFO tracking
    buy_lots = []  # FIFO queue for purchases: [{'amount': float, 'price': float, 'cost': float, 'timestamp': float}]
    sell_queue = []  # Track unmatched sells
    
    realized_pnl = 0.0
    total_bought = 0.0
    total_sold = 0.0
    trade_count = 0
    winning_trades = 0
    total_holding_time_seconds = 0.0
    latest_price = 0.0
    
    for txn in txn_list:
        try:
            tx_type = txn.get('transaction_type', '').upper()
            from_amount = float(txn.get('from_amount') or 0)
            to_amount = float(txn.get('to_amount') or 0)
            base_price = float(txn.get('base_price') or 0)
            quote_price = float(txn.get('quote_price') or 0)
            value_usd = float(txn.get('value_usd') or 0)
            timestamp = txn.get('timestamp')
            
            # Handle timestamp conversion
            if hasattr(timestamp, 'timestamp'):
                timestamp_unix = timestamp.timestamp()
            else:
                timestamp_unix = float(timestamp) if timestamp else 0
            
            # Determine price
            price = base_price or quote_price or 0
            if price > 0:
                latest_price = price
            
            # Enhanced BUY processing
            if tx_type == 'BUY' and to_amount > 0:
                # Calculate cost basis and price
                if value_usd > 0:
                    cost_basis = value_usd
                    buy_price = cost_basis / to_amount
                elif price > 0:
                    buy_price = price
                    cost_basis = to_amount * buy_price
                else:
                    # Skip if no price info
                    continue
                
                total_bought += cost_basis
                
                # Add to buy lots queue
                buy_lots.append({
                    'amount': to_amount,
                    'price': buy_price,
                    'cost_basis': cost_basis,
                    'timestamp': timestamp_unix
                })
                
                # Try to match against any pending sells
                while sell_queue and buy_lots:
                    sell_order = sell_queue[0]
                    buy_lot = buy_lots[0]
                    
                    if sell_order['amount'] <= buy_lot['amount']:
                        # Sell entire pending order against this buy lot
                        sell_amount = sell_order['amount']
                        sell_value = sell_amount * sell_order['price']
                        cost_basis_used = (sell_amount / buy_lot['amount']) * buy_lot['cost_basis']
                        
                        lot_pnl = sell_value - cost_basis_used
                        realized_pnl += lot_pnl
                        
                        trade_count += 1
                        if lot_pnl > 0:
                            winning_trades += 1
                        
                        # Calculate holding time (sell time - buy time)
                        holding_time = sell_order['timestamp'] - buy_lot['timestamp']
                        total_holding_time_seconds += max(0, holding_time)
                        
                        # Update lots
                        buy_lot['amount'] -= sell_amount
                        buy_lot['cost_basis'] -= cost_basis_used
                        
                        if buy_lot['amount'] <= 0.001:  # Almost zero, remove
                            buy_lots.pop(0)
                        
                        sell_queue.pop(0)
                    else:
                        # Partial sell - use entire buy lot
                        buy_amount = buy_lot['amount']
                        sell_value = buy_amount * sell_order['price']
                        cost_basis_used = buy_lot['cost_basis']
                        
                        lot_pnl = sell_value - cost_basis_used
                        realized_pnl += lot_pnl
                        
                        trade_count += 1
                        if lot_pnl > 0:
                            winning_trades += 1
                        
                        holding_time = sell_order['timestamp'] - buy_lot['timestamp']
                        total_holding_time_seconds += max(0, holding_time)
                        
                        # Update orders
                        sell_order['amount'] -= buy_amount
                        buy_lots.pop(0)
            
            # Enhanced SELL processing
            elif tx_type == 'SELL' and from_amount > 0:
                # Calculate sale value and price
                if value_usd > 0:
                    sale_value = value_usd
                    sell_price = sale_value / from_amount
                elif price > 0:
                    sell_price = price
                    sale_value = from_amount * sell_price
                else:
                    # Skip if no price info
                    continue
                
                total_sold += sale_value
                remaining_to_sell = from_amount
                
                # Try to match against existing buy lots
                while remaining_to_sell > 0.001 and buy_lots:
                    buy_lot = buy_lots[0]
                    
                    if buy_lot['amount'] <= remaining_to_sell:
                        # Use entire buy lot
                        lot_amount = buy_lot['amount']
                        lot_sale_value = lot_amount * sell_price
                        lot_pnl = lot_sale_value - buy_lot['cost_basis']
                        realized_pnl += lot_pnl
                        
                        trade_count += 1
                        if lot_pnl > 0:
                            winning_trades += 1
                        
                        holding_time = timestamp_unix - buy_lot['timestamp']
                        total_holding_time_seconds += max(0, holding_time)
                        
                        remaining_to_sell -= lot_amount
                        buy_lots.pop(0)
                    else:
                        # Partial lot usage
                        sell_fraction = remaining_to_sell / buy_lot['amount']
                        cost_basis_used = buy_lot['cost_basis'] * sell_fraction
                        lot_sale_value = remaining_to_sell * sell_price
                        lot_pnl = lot_sale_value - cost_basis_used
                        realized_pnl += lot_pnl
                        
                        trade_count += 1
                        if lot_pnl > 0:
                            winning_trades += 1
                        
                        holding_time = timestamp_unix - buy_lot['timestamp']
                        total_holding_time_seconds += max(0, holding_time)
                        
                        # Update remaining lot
                        buy_lot['amount'] -= remaining_to_sell
                        buy_lot['cost_basis'] -= cost_basis_used
                        remaining_to_sell = 0
                
                # If there's still remaining to sell, add to sell queue
                if remaining_to_sell > 0.001:
                    sell_queue.append({
                        'amount': remaining_to_sell,
                        'price': sell_price,
                        'timestamp': timestamp_unix
                    })
                    
        except Exception as e:
            # Skip problematic transactions but continue processing
            continue
    
    # Calculate current position (remaining buy lots)
    current_position_tokens = sum(lot['amount'] for lot in buy_lots)
    current_position_cost_basis = sum(lot['cost_basis'] for lot in buy_lots)
    avg_buy_price = (current_position_cost_basis / current_position_tokens) if current_position_tokens > 0 else 0.0
    
    # Calculate unrealized PnL
    unrealized_pnl = 0.0
    if current_position_tokens > 0 and latest_price > 0:
        current_value = current_position_tokens * latest_price
        unrealized_pnl = current_value - current_position_cost_basis
    
    # Average holding time
    avg_holding_time_hours = (total_holding_time_seconds / 3600.0 / trade_count) if trade_count > 0 else 0.0
    
    return (
        realized_pnl,
        unrealized_pnl,
        total_bought,
        total_sold,
        trade_count,
        winning_trades,
        avg_holding_time_hours,
        current_position_tokens,
        current_position_cost_basis,
        avg_buy_price,
        latest_price
    )