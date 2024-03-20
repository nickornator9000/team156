#this file is to inject the yaml config
import yaml
class SingletonClass(object):
  
  def __init__(self):
    self.singleConfig = self.getConfig()
  def __new__(cls):
    if not hasattr(cls, 'instance'):
      cls.instance = super(SingletonClass, cls).__new__(cls)
    return cls.instance
  
  @staticmethod
  def getConfig():
    #need to pass in path access
    pass