require "test_helper"

class DdlEventsTest < GhostferryTestCase
  DDL_GHOSTFERRY = "ddl_ghostferry"

  def test_default_event_handler
    seed_simple_database_with_single_table

    #datawriter = new_source_datawriter

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    #start_datawriter_with_ghostferry(datawriter, ghostferry)
    #stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run_with_logs()

    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_ddl_event_handler
    #skip("skipping")
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(DDL_GHOSTFERRY)
    ghostferry.run_with_logs()

    assert_ghostferry_completed(ghostferry, times: 1)
  end
end
