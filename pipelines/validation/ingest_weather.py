import great_expectations as gx


def validate(df):

    context = gx.get_context()
    validator = context.get_validator(df=df)
    validator.expect_column_values_to_not_be_null("timestamp")
    validator.expect_column_values_to_be_between(
        "temperature",
        min_value=-80,
        max_value=60,
    )

    return validator.validate()
