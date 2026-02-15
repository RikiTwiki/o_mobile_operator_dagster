from dagster import ConfigurableResource

class MPDailyReportConfigResource(ConfigurableResource):
    position_ids: str = "10, 34, 41, 46"   #"49,50,51"
    project_ids: str = "1,4,5,6,10,11,12,15"  # 1,

    def get_position_ids(self):
        return self.position_ids.split(',')

    def get_project_ids(self):
        return self.project_ids.split(',')
