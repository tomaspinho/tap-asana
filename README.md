# tap-asana

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Asana v.1.0 API](https://developers.asana.com/docs)
- Extracts the following resources:
  - [Portfolios](https://developers.asana.com/docs/portfolios) with [Portfolio Items](https://developers.asana.com/docs/get-portfolio-items)
    - Only available for [Business and Enterprise Subscriptions](https://asana.com/pricing)
  - [Projects](https://developers.asana.com/docs/projects)
  - [Sections](https://developers.asana.com/docs/sections)
  - [Stories](https://developers.asana.com/docs/stories)
  - [Tags](https://developers.asana.com/docs/tags)
  - [Tasks](https://developers.asana.com/docs/tasks)
  - [Teams](https://developers.asana.com/docs/teams)
  - [Users](https://developers.asana.com/docs/users)
  - [Workspaces](https://developers.asana.com/docs/workspaces)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

```
$ git clone https://github.com/j-oflaherty/tap-asana.git tap-asana
$ cd tap-asana
$ pip install -e '.[dev]'
```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

```json
{
  "client_id": "111",
  "client_secret": "xxx",
  "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
  "refresh_token": "yyy",
  "start_date" : "2018-02-22T02:06:58.147Z"
}
```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `client_id`, `client_secret`, `redirect_uri`, and `refresh_token` can be generated following these [Asana OAuth instructions](https://developers.asana.com/docs/oauth).

4. Run the Tap in Discovery Mode

    tap-asana -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-asana -c config.json --catalog catalog-file.json

## Development

First, clone this repo. Then, in the directory:

```
$ mkvirtualenv -p python3 tap-asana
$ make dev
```

## Modification

To go around the Bearer token expiration error the script has been modified to refresh it's token if 45 minutes have passed since the last update. 

---

Copyright &copy; 2019 Stitch
