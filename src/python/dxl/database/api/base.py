from ..model import Database as DBM
from .. import base
from .. import web
from ..config import config as c

class Base():
    @classmethod
    def setUp(self):
        # c = config
        #c['path'] = ':memory:'
        DBM.create()

    def tearDown():
        Database.clear()
        config.config.back_to_default()

    # @unittest.skip
    # def test_create_engine(self):
    #     Database().get_or_create_engine()

    # def test_create_database(self):
    #     Database.create()
    #     #database.Database.create()
    #     Database.clear()

    # @unittest.skip
    # def test_get_session(self):
    #     sess = database.Database().session()
    #     sess.query(database.TaskDB).all()
    #     sess.close()
    def create(s):
        if c['use_web_api']:
            return web.create(s)
        else:
            return base.DBprocess.create(s)

    def read(tid):
        if c['use_web_api']:
            return web.read(tid)
        else:
            return base.DBprocess.read(tid)

    def read_all():
        if c['use_web_api']:
            return web.read_all()
        else:
            return base.DBprocess.read_all()


    def update(s):
        if c['use_web_api']:
            return web.update(s)
        else:
            return base.DBprocess.update(s)


    def delete(tid):
        if c['use_web_api']:
            return web.delete(tid)
        else:
            return base.DBprocess.delete(tid)

