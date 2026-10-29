# Vendored third-party code -- do not edit; replace the whole file to update.
#
#   Project:  Flask-Sock, WebSocket support for Flask
#   Version:  0.7.0
#   URL:      https://github.com/miguelgrinberg/flask-sock
#   Author:   Miguel Grinberg
#   Licence:  MIT (full text below)
#
# Vendored because Flask-Sock is in no Debian or Ubuntu archive. The code
# after the licence is the upstream flask_sock/__init__.py, byte for byte.
#
# MIT License
#
# Copyright (c) 2021 Miguel Grinberg
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from functools import wraps
from flask import Blueprint, request, Response, current_app
from simple_websocket import Server, ConnectionClosed


class Sock:
    """Instantiate the Flask-Sock extension.

    :param app: The Flask application instance. If not provided, it must be
                initialized later by calling the :func:`Sock.init_app` method.
    """
    def __init__(self, app=None):
        self.app = None
        self.bp = None
        if app is not None:
            self.app = app
            self.init_app(app)

    def init_app(self, app):
        """Initialize the Flask-Socket extension.


        :param app: The Flask application instance. This method only needs to
                    be called if the application instance was not passed as
                    an argument to the constructor.
        """
        if self.bp:
            app.register_blueprint(self.bp)
        self.app = app

    def route(self, path, bp=None, **kwargs):
        """Decorator to create a WebSocket route.

        The decorated function will be invoked when a WebSocket client
        establishes a connection, with a WebSocket connection object passed
        as an argument. Example::

            @sock.route('/ws')
            def websocket_route(ws):
                # The ws object has the following methods:
                # - ws.send(data)
                # - ws.receive(timeout=None)
                # - ws.close(reason=None, message=None)

        If the route has variable components, the ``ws`` argument needs to be
        included before them.

        :param path: the URL associated with the route.
        :param bp: the blueprint on which to register the route. If not given,
                   the route is attached directly to the Flask application
                   instance. When a blueprint is used, the application is
                   responsible for the blueprint's registration.
        :param kwargs: additional route options. See the Flask documentation
                       for the ``app.route`` decorator for details.
        """
        def decorator(f):
            @wraps(f)
            def websocket_route(*args, **kwargs):  # pragma: no cover
                ws = Server(request.environ, **current_app.config.get(
                    'SOCK_SERVER_OPTIONS', {}))
                try:
                    f(ws, *args, **kwargs)
                except ConnectionClosed:
                    pass
                try:
                    ws.close()
                except:  # noqa: E722
                    pass

                class WebSocketResponse(Response):
                    def __call__(self, *args, **kwargs):
                        if ws.mode == 'eventlet':
                            try:
                                from eventlet.wsgi import WSGI_LOCAL
                                ALREADY_HANDLED = []
                            except ImportError:
                                from eventlet.wsgi import ALREADY_HANDLED
                                WSGI_LOCAL = None

                            if hasattr(WSGI_LOCAL, 'already_handled'):
                                WSGI_LOCAL.already_handled = True
                            return ALREADY_HANDLED
                        elif ws.mode == 'gunicorn':
                            raise StopIteration()
                        elif ws.mode == 'werkzeug':
                            return super().__call__(*args, **kwargs)
                        else:
                            return []

                return WebSocketResponse()

            kwargs['websocket'] = True
            if bp:
                bp.route(path, **kwargs)(websocket_route)
            elif self.app:
                self.app.route(path, **kwargs)(websocket_route)
            else:
                if self.bp is None:  # pragma: no branch
                    self.bp = Blueprint('__flask_sock', __name__)
                self.bp.route(path, **kwargs)(websocket_route)

        return decorator
