from ede.database import session, app_engine, Base
import ede.models
import ede.settings
from sqlalchemy.exc import IntegrityError
from argparse import ArgumentParser
from ede.tasks import hello_world

def init_db(args={}):
    if args.everything:
        init_master_meta_user()
        init_celery()
    else:
        if args.tables:
            init_master_meta_user()
        if args.celery:
            init_celery()

def init_master_meta_user():
    print 'creating master, meta and user tables'
    Base.metadata.create_all(bind=app_engine)
    if ede.settings.DEFAULT_USER:
        print 'creating default user %s' % ede.settings.DEFAULT_USER['name']
        user = ede.models.User(**ede.settings.DEFAULT_USER)
        session.add(user)
        try:
            session.commit()
        except IntegrityError:
            pass

def init_celery():
    hello_world.delay()

def build_arg_parser():
    """
    Creates an argument parser for this script. This is helpful in the event
    that a user needs to only run a portion of the setup script.
    """

    description = 'Set up your development environment with this script. It \
    creates tables.'
    parser = ArgumentParser(description=description)
    parser.add_argument('-t', '--tables', dest='tables', help='Set up the \
            master, meta and user tables')
    parser.add_argument('-cl', '--celery', dest='celery', help='Say hello \
            world from Celery')
    parser.add_argument('-e', '--everything', dest='everything', help='Run \
            everything in the script.', default=True)
    return parser

if __name__ == "__main__":
    parser = build_arg_parser()
    arguments = parser.parse_args()
    init_db(arguments)
