import asana
import singer
import datetime

LOGGER = singer.get_logger()

""" Simple wrapper for Asana. """


class Asana(object):

    def __init__(self, client_id, client_secret, redirect_uri, refresh_token, start_date, access_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token = refresh_token
        self.access_token = access_token
        self._client = self._oauth_auth() or self._access_token_auth()
        self._last_refresh = datetime.datetime(year=2000, month=1, day=1)  # Fecha anterior a creacion de asana
        self.maybe_refresh_access_token()

    def _oauth_auth(self):
        if self.client_id is None or self.client_secret is None or self.redirect_uri is None or self.refresh_token is None:
            LOGGER.debug("OAuth authentication unavailable.")
            return None
        return asana.Client.oauth(client_id=self.client_id, client_secret=self.client_secret,
                                  redirect_uri=self.redirect_uri)

    def _access_token_auth(self):
        if self.access_token is None:
            LOGGER.debug("OAuth authentication unavailable.")
            return None
        return asana.Client.access_token(self.access_token)

    def maybe_refresh_access_token(self):
        now = datetime.datetime.now()
        if now - self._last_refresh < datetime.timedelta(minutes=45):
            return
        LOGGER.info("Refreshing access token at %s.", now.strftime("%H:%M:%S"))
        self._last_refresh = now
        return self._client.session.refresh_token(self._client.session.token_url, client_id=self.client_id,
                                                  client_secret=self.client_secret, refresh_token=self.refresh_token)

    @property
    def client(self):
        return self._client
