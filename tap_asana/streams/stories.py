
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream
import singer

LOGGER = singer.get_logger()

class Stories(Stream):
  name = "stories"
  replication_key = "created_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "gid",
    "resource_type",
    "created_at",
    "created_by",
    "resource_subtype",
    "text",
    "html_text",
    "is_pinned",
    "assignee",
    "dependency",
    "duplicate_of",
    "duplicated_from",
    "follower",
    "hearted",
    "hearts",
    "is_edited",
    "liked",
    "likes",
    "new_approval_status",
    "new_dates",
    "new_enum_value",
    "new_name",
    "new_number_value",
    "new_resource_subtype",
    "new_section",
    "new_text_value",
    "num_hearts",
    "num_likes",
    "old_approval_status",
    "old_dates",
    "old_enum_value",
    "old_name",
    "old_number_value",
    "old_resource_subtype",
    "old_section",
    "old_text_value",
    "preview",
    "project",
    "source",
    "story",
    "tag",
    "target",
    "task"
  ]


  def get_objects(self):
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    opt_fields = ",".join(self.fields)

    LOGGER.info("Syncing stream stories: getting workspaces")
    workspaces = [w for w in self.call_api("workspaces")]

    LOGGER.info("Syncing stream stories: getting projects")
    projects = [p for workspace in workspaces for p in self.call_api("projects", workspace=workspace["gid"])]
    projects_len = len(projects)

    LOGGER.info("Syncing stream stories: getting tasks")
    tasks = []

    for i, project in enumerate(projects):
      Context.asana.maybe_refresh_access_token()

      project_gid = project['gid']
      num_tasks = Context.asana.client.projects.get_task_counts_for_project(project_gid, opt_pretty=True, opt_fields="num_tasks")["num_tasks"]

      LOGGER.info("Syncing stream stories: getting %d tasks for project %s, %d/%d", num_tasks, project_gid, i+1, projects_len)

      for t in self.call_api("tasks", project=project["gid"]):
        tasks.append(t)

    LOGGER.info("Syncing stream stories: getting tasks's stories")
    tasks_len = len(tasks)
    for i, task in enumerate(tasks):
      Context.asana.maybe_refresh_access_token()
      task_gid = task.get('gid')

      LOGGER.info("Syncing stream stories: getting stories for task %s, %d/%d tasks", task_gid, i+1, tasks_len)

      for story in Context.asana.client.stories.get_stories_for_task(task_gid=task_gid, opt_fields=opt_fields):
        session_bookmark = self.get_updated_session_bookmark(session_bookmark, story[self.replication_key])
        if self.is_bookmark_old(story[self.replication_key]):
          yield story
    self.update_bookmark(session_bookmark)


Context.stream_objects['stories'] = Stories
