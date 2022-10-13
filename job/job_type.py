
from pydantic import BaseModel

class SingleItemAllocation(BaseModel):
    '单个待处理条目所需要的资源'
    node: int
    memory: int
    unit: str

class Submit(BaseModel):
    user: str
    data_dir: str
    execute_file_path: str
    resource_per_item: SingleItemAllocation