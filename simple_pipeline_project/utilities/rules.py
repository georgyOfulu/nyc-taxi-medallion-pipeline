
def get_rules_dict():
    return [
        {
            'name': 'correct_data_type',
            'condition': '''
                            (VendorID IS NOT NULL AND VendorID RLIKE '^[0-9]+$' AND
                            payment_type IS NOT NULL AND payment_type RLIKE '^[0-9]+$' AND
                            total_amount RLIKE '^[-+]?[0-9]*\\.?[0-9]+$' AND
                            trip_month IS NOT NULL AND trip_month RLIKE '^\\d{4}-\\d{2}-\\d{2}$')
                            ''',
            'tag': 'validity'
        },
        {
            'name': 'valid_payment_type',
            'condition': '''
                payment_type RLIKE '^[0-9]+$' AND
                payment_type BETWEEN 1 AND 5''',
            'tag': 'validity'
        }
    ]