import GafferUI

import GafferDeadline
import GafferDeadlineUI


nodeMenu = GafferUI.NodeMenu.acquire(application)

nodeMenu.append("/Dispatch/Deadline Dispatcher", GafferDeadline.DeadlineDispatcher, searchText="DeadlineDispatcher")
nodeMenu.append("/Deadline/DeadlineTask", GafferDeadline.DeadlineTask, searchText="DeadlineTask")
