require "test_helper"

class DdlEventsTest < GhostferryTestCase
  # DDL_FERRY = "ddl_ghostferry"

  def test_default_event_handler
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run

    assert_ghostferry_completed(ghostferry, times: 1)
  end
end
