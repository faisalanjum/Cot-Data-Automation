from backend.db.dbconnect import connect_to_database
from backend.db.models import  Base


#funciton to migrate the code
def migrate_models():
    engine=connect_to_database(get_engine_only=True)

    try:
        Base.metadata.create_all(engine)
        print("migrations completed")

    except Exception as e:
        raise e



#funciton to migrate the code
def clean_db():
    engine=connect_to_database(get_engine_only=True)

    try:
        Base.metadata.drop_all(engine)
        print("database cleaned completed")

    except Exception as e:
        raise e


# clean_db()

# migrate_models()