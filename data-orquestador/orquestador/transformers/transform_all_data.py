if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    if isinstance(data, bool):
        return False
    data.drop_duplicates(inplace=True) # Removes duplicates

    data["is_free_trip"] = (data["total_amount"] == 0) & (data["trip_distance"] > 0) # Free trips with distance > 0
    data["is_cancelled_trip"] = (data["total_amount"] == 0) & (data["trip_distance"] == 0) # Cancelled trips with distance = 0
    data["trip_duration"] = (data["tpep_dropoff_datetime"] - data["tpep_pickup_datetime"]).dt.total_seconds() #calculate trip duration in seconds
    
    data = data[data["trip_duration"] >= 0] # Remove trips with negative duration (dropoff before pickup)
    data = data[data["fare_amount"] >= 0] # Remove trips with negative fare amount
    data = data[data["trip_distance"] >= 0] # Remove trips with negative distance
    data.loc[data["trip_distance"] > 0, "price_per_mile"] = data["total_amount"] / data["trip_distance"] # Calculate price per mile for trips with distance > 0
    data = data[~(data['tpep_pickup_datetime'] == data['tpep_dropoff_datetime']) & (data["price_per_mile"].notna())] # Remove trips with zero duration but non-zero price per mile

    # flag possible outliers based on IQR
    q1 = data["price_per_mile"].quantile(0.25)
    q3 = data["price_per_mile"].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    data["is_outlier"] = ((data["price_per_mile"] < lower_bound) | (data["price_per_mile"] > upper_bound)) # Flag outliers based on price per mile
    v = data["is_outlier"].mean()
    if ( v > 0.05):
        print(f"Warning: {v:.2%} of trips are outliers based on price per mile.")
    else:
        print(f"Only {v:.2%} of trips are outliers based on price per mile, which is acceptable.")

    values = data.isna().sum() / data.shape[0]
    data.drop(columns=values[values > 0.5].index, inplace=True) # Removes columns with 50% of null values

    # A good practice would be to use PCA to find reads out of the normal
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    if not isinstance(output, bool):
        assert output is not None, 'The output is undefined'