import GafferUI

import GafferDeadline
import GafferDeadlineUI


nodeMenu = GafferUI.NodeMenu.acquire(application)

nodeMenu.append("/Deadline/DeadlineTask", GafferDeadline.DeadlineTask, searchText="DeadlineTask")
