import asyncio
from DataTransitionManager import DATA_TRN_MNG
from config import log
from config import target_groups_ids

class Controller:
    def __init__(self, initGroups: dict = dict()):
        self.groups: dict = initGroups
        self.loop = asyncio.get_running_loop()
    
    async def clear(self):
        for task in self.groups.items:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            log.debug("removed all targets for pulling")
            self.groups.clear()
                

    async def add(self, group_id: str):
        if self.groups.get(group_id) != None:
            return

        task = self.loop.create_task(groupPullingFactoryTask(group_id, sleepTime=5))
        self.groups[group_id] = task
        log.debug(f"added for pulling: {group_id}")
        
    async def addMultiple(self, group_ids: set):
        for group_id in group_ids: 
            await self.add(group_id=group_id)

    async def remove(self, group_id: str):
        if self.groups.get(group_id) != None:
            self.groups[group_id].cancel()
            try:
                await self.groups[group_id]
            except asyncio.CancelledError:
                log.debug(f"removed for pulling: {group_id}")
                self.groups.pop(group_id)

async def groupPullingFactoryTask(group_id, sleepTime):
    loop = asyncio.get_running_loop()
    while True:
        loop.create_task(DATA_TRN_MNG.pullDataAndSend(group_id=group_id))
        await asyncio.sleep(sleepTime)   

async def controllerTask():
    controller = Controller()
    # await controller.add("public192867633")
    await controller.addMultiple(target_groups_ids)


def addGroupToObserve(self):
    self.__sendCommandToManager()

def addAPIToken(self):
    pass




