from slowstream import ConsumerApp


def test_assert(kafka):
    app = ConsumerApp(
        bootstrap_servers='',
        group_id='...',
    )
