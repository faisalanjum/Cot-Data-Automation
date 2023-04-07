import sys
import pathlib
from db.dbconnect import connect_to_database as db_cot

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

class DataRetreivalControllerCOT:

    def __init__(self):
        """
        Initializes the database session
        """
        self.session = db_cot()

    def query_data(self, mapper, parameter_val=None, parameter="id"):
        """
        Queries data from the database using the given mapper and parameter(s)

        Args:
            mapper: The mapper to query from
            parameter_val: The value(s) of the parameter to use in the query (optional)
            parameter: The name of the parameter to use in the query (default is "id")

        Returns:
            A list of dictionaries containing the queried data
        """
        session = self.session()

        if type(parameter_val) == str:
            q_res = session.query(mapper).filter(getattr(mapper, parameter) == parameter_val).all()
            res = [q.toDict() for q in q_res]
            return res

        elif type(parameter_val) == list:
            prm = getattr(mapper, parameter)
            q_res = session.query(mapper).filter(prm.in_(parameter_val)).all()
            res = [q.toDict() for q in q_res]
            return res

        elif parameter_val is None:
            q_res = session.query(mapper).all()
            res = [q.toDict() for q in q_res]
            return res

        else:
            print("enter valid par_value str and list is valid type ")


    

    def get_latest_series(self, mapper, parameter_val=None, parameter="id"):
        """
        Retrieves the latest series from the database.

        Args:
            mapper: The SQLAlchemy mapper representing the table to retrieve data from.
            parameter_val: The value of the parameter to filter on.
            parameter: The name of the parameter to filter on.

        Returns:
            The latest series as a dictionary.
        """

        session = self.session()

        if type(parameter_val) == str:
            q_res = session.query(mapper).filter(getattr(mapper, parameter) == parameter_val).order_by(mapper.date.desc()).first()
            res = q_res.toDict()
            return res

        elif type(parameter_val) == list:
            prm = getattr(mapper, parameter)
            q_res = session.query(mapper).filter(prm.in_(parameter_val)).order_by(mapper.date.desc()).first()
            res = q_res.toDict()
            return res

        elif parameter_val is None:
            q_res = session.query(mapper).order_by(mapper.date.desc()).first()
            res = q_res.toDict()
            return res

        else:
            print("Enter valid parameter_val; str and list are valid types.")

    
            

    
            