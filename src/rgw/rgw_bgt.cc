// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "common/ceph_json.h"
#include "common/utf8.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"
#include "common/Finisher.h"

#include "rgw_rados.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_metadata.h"
#include "rgw_bucket.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/statelog/cls_statelog_client.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/user/cls_user_client.h"

#include "rgw_tools.h"

#include "common/Clock.h"

#include "include/rados/librados.hpp"
using namespace librados;
#if 0
#include "include/radosstriper/libradosstriper.hpp"
using namespace libradosstriper;
#endif
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>
#include "auth/Crypto.h" // get_random_bytes()

#include "rgw_log.h"

#include "rgw_gc.h"
#include "rgw_bgt.h"

#define dout_subsys ceph_subsys_rgw


using namespace std;


#define dout_subsys ceph_subsys_rgw

std::string rgw_unique_lock_name(const std::string &name, void *address) 
{
  return name + " (" + stringify(address) + ")";
}

std::ostream &operator<<(std::ostream &out, const RGW_BGT_WORKER_STATE &state)
{
	switch (state)
	{
		case RGW_BGT_WORKER_INACTIVE:
			out << "inactive";
			break;
			
		case RGW_BGT_WORKER_ACTIVE:
			out << "active";
			break;
			
		default:
		  out << "unknown";
		  break;
	}

	return out;
}

std::ostream &operator<<(std::ostream &out, const RGWInstObjNotifyOp &op)
{
	switch (op)
	{
		case RGW_NOTIFY_OP_REGISTER:
			out << "WorkerRegister";
			break;

		case RGW_NOTIFY_OP_REGISTER_RSP:
			out << "WorkerRegisterRsp";
			break;
	
		case RGW_NOTIFY_OP_ACTIVE_CHANGELOG:
			out << "ActiveChangeLog";
			break;

		case RGW_NOTIFY_OP_ACTIVE_CHANGELOG_RSP:
			out << "ActiveChangeLogRsp";
			break;
	
		case RGW_NOTIFY_OP_DISPATCH_TASK:
			out << "DispatchTask";
			break;

		case RGW_NOTIFY_OP_DISPATCH_TASK_RSP:
			out << "DispatchTaskRsp";
			break;
	
		case RGW_NOTIFY_OP_TASK_FINISH:
			out << "TaskFinished";
			break;

		case RGW_NOTIFY_OP_TASK_FINISH_RSP:
			out << "TaskFinishedRsp";
			break;
	
		default:
			out << "Unknown (" << static_cast<uint32_t>(op) << ")";
			break;
	}

	return out;	
}

std::ostream &operator<<(std::ostream &out, const RGWBgtTaskState &state)
{
	switch (state)
	{
		case RGW_BGT_TASK_WAIT_CREATE_SFMOBJ:
			out << "wait create sfm obj";
			break;
			
		case RGW_BGT_TASK_WAIT_MERGE:
			out << "wait merge";
			break;
			
		case RGW_BGT_TASK_WAIT_UPDATE_INDEX:
			out << "wait update index of small obj";
			break;
			
		case RGW_BGT_TASK_WAIT_DEL_DATA:
			out << "wait del data of small obj";
			break;
			
		case RGW_BGT_TASK_WAIT_REPORT_FINISH:
			out << "wait report finish";
			break;
			
		case RGW_BGT_TASK_FINISH:
			out << "task finished";
			break;
			
		default:
			out << "unkonown";
			break;
	}

	return out;
}

std::ostream &operator<<(std::ostream &out, const RGWBgtTaskSFMergeState &state)
{
	switch (state)
	{
		case RGW_BGT_TASK_SFM_START:
			out << "SFM start";
			break;
			
		case RGW_BGT_TASK_SFM_INDEX_CREATED:
			out << "SFM index created";
			break;
			
		case RGW_BGT_TASK_SFM_MERGED:
			out << "SFM data merged";
			break;
			
		case RGW_BGT_TASK_SFM_DATA_FLUSHED:
			out << "SFM data flushed";
			break;
			
		case RGW_BGT_TASK_SFM_INDEX_FLUSHED:
			out << "SFM index flushed";
			break;
			
		default:
			out << "unknown";
			break;
	}

	return out;
}

std::ostream &operator<<(std::ostream &out, const RGWBgtBatchTaskState &state)
{
	switch (state)
	{
		case RGW_BGT_BATCH_TASK_WAIT_CREATE:
			out << "wait create";
			break;
			
		case RGW_BGT_BATCH_TASK_WAIT_DISPATCH:
			out << "wait dispatch";
			break;
			
		case RGW_BGT_BATCH_TASK_WAIT_RM_LOG:
			out << "wait remove log file";
			break;
			
		case RGW_BGT_BATCH_TASK_WAIT_RM_LOG_ENTRY:
			out << "wait remove log entry";
			break;
			
		case RGW_BGT_BATCH_TASK_WAIT_CLEAR_TASK_ENTRY:
			out << "wait clear task entry";
			break;
			
		case RGW_BGT_BATCH_TASK_FINISH:
			out << "batch task finish";
			break;
			
		default:
			out << "unknown";
			break;
	}

	return out;
}

std::ostream &operator<<(std::ostream &out, const RGWBgtLogTransState &state)
{
	switch (state)
	{
		case RGW_BGT_LOG_TRANS_START:
			out << "log trans start";
			break;
			
		case RGW_BGT_LOG_TRANS_WAIT_CREATE_LOG_OBJ:
			out << "wait create log obj";
			break;
			
		case RGW_BGT_LOG_TRANS_WAIT_ADD_TO_INACTIVE:
			out << "wait add active log to inactive log";
			break;
			
		case RGW_BGT_LOG_TRANS_WAIT_SET_ACTIVE:
			out << "wait set new active log";
			break;
			
		case RGW_BGT_LOG_TRANS_WAIT_NOTIFY_WORKER:
			out << "wait broadcast new active log";
			break;
			
		case RGW_BGT_LOG_TRANS_FINISH:
			out << "log trans finish";
			break;
			
		default:
			out << "unknown";
			break;
	}

	return out;
}

void RGWInstObjWatchCtx::handle_notify(uint64_t notify_id,uint64_t handle, uint64_t notifier_id,  bufferlist& bl)
{
  /*guokexin*/

   ldout(m_cct, 10) << "NOTIFY"
                   << " handle " << handle
                   << " notify_id " << notify_id
                   << " from " << notifier_id
                   << dendl;
#if 0
   bl.hexdump(cout);
   m_instobj_watcher->m_instobj->m_io_ctx.notify_ack(m_instobj_watcher->m_instobj->m_name, notify_id, handle, bl);
#endif   
  return m_instobj_watcher->handle_notify(notify_id, handle, bl);
}

void RGWInstObjWatchCtx::handle_error(uint64_t handle, int err)
{
#if 0
   ldout(m_cct, 0) << "ERROR"
                   << " handle " << handle
                   << " err " << cpp_strerror(err)
                   << dendl;
#endif     
  return m_instobj_watcher->handle_error(handle, err);
}

RGWInstanceObj::RGWInstanceObj(const std::string &pool_name, const std::string &instobj_name, 
                                     RGWRados* store, CephContext* cct) : 
                                     m_cct(cct),
                                     m_store(store),
                                     m_pool(pool_name),
                                     m_name(instobj_name)
{

}

RGWInstanceObj::~RGWInstanceObj()
{
}

int RGWInstanceObj::init()
{
  librados::Rados *rad = m_store->get_rados_handle();
  int r = rad->ioctx_create(m_pool.c_str(), m_io_ctx);
  if (r < 0)
  {
    ldout(m_cct, 0) << __func__ << "error opening pool " << m_pool << ": "
	                  << cpp_strerror(r) << dendl;  
    return r;
  }
#if 0  
  r = libradosstriper::RadosStriper::striper_create(m_io_ctx, &m_striper);
  if (0 != r)
  {
    ldout(m_cct, 0) << "error opening pool " << m_pool << " with striper interface: "
                    << cpp_strerror(r) << dendl;  
    return r;
  }
#endif  
  r = m_io_ctx.create(m_name, true);
  if (r < 0 && -EEXIST != r)
  {
    ldout(m_cct, 0) << "error create obj " << m_name << " in pool " << m_pool << ": "
                    << cpp_strerror(r) << dendl;
    return r;
  }
  
  if (-EEXIST == r)
  {
    ldout(m_cct, 10) << "obj " << m_name << " has existed in pool " << m_pool << ": "
                    << cpp_strerror(r) << dendl; 
    r = 0;                
  }
  
  return r;
}

int RGWInstanceObj::register_watch(uint64_t* handle, RGWInstObjWatchCtx* ctx)
{
  return m_io_ctx.watch2(m_name, handle, ctx);
}

int RGWInstanceObj::unregister_watch(uint64_t handle)
{
  return m_io_ctx.unwatch2(handle);
}

RGWInstanceObjWatcher::RGWInstanceObjWatcher(RGWInstanceObj* instobj, int role, RGWRados* store, CephContext* cct) : 
                                                     m_cct(cct),
                                                     m_store(store),
                                                     m_instobj(instobj), 
                                                     m_watch_ctx(this, cct),
                                                     m_watch_handle(0),
                                                     m_watch_state(0),
                                                     m_state(0),
                                                     m_role(role)
{
}

RGWInstanceObjWatcher::~RGWInstanceObjWatcher()
{
  if (NULL != m_instobj)
  {
    if (0 != m_watch_state)
    {
      librados::Rados *rad = m_store->get_rados_handle();
      rad->watch_flush();
      
      unregister_watch();
    } 

    delete m_instobj;
    m_instobj = NULL;
  }
}

int RGWInstanceObjWatcher::register_watch()
{
  int ret = m_instobj->register_watch(&m_watch_handle, &m_watch_ctx);
  if (ret < 0)
  {
    return ret;
  }

  m_watch_state = RGW_INSTOBJWATCH_STATE_REGISTERED;
  return 0;
}

int RGWInstanceObjWatcher::unregister_watch()
{
  int ret = m_instobj->unregister_watch(m_watch_handle);
  if (ret < 0)
  {
    return ret;
  }

  m_watch_state = RGW_INSTOBJWATCH_STATE_UNREGISTERED;

  return 0;
}

int RGWInstanceObjWatcher::get_notify_rsp(bufferlist& ack_bl, RGWInstObjNotifyRspMsg& notify_rsp)
{
  typedef std::map<std::pair<uint64_t, uint64_t>, bufferlist> responses_t;
  responses_t responses;
  try
  {
    bufferlist::iterator iter = ack_bl.begin();
    ldout(m_cct, 20) << "ack_bl size:" << ack_bl.length() << dendl;
    ::decode(responses, iter);
  }catch(const buffer::error &err)
  {
    ldout(m_cct, 0) << " error decoding responses: "
                    << err.what() << dendl;  
    return -1;                
  }

  bufferlist response;
  for (responses_t::iterator i = responses.begin(); i != responses.end(); ++i)
  {
    if (i->second.length() > 0)
    {
      response.claim(i->second);
    }
  }

  try
  {
    ldout(m_cct, 20) << "response size:" << response.length() << dendl;
    bufferlist::iterator iter = response.begin();
    ::decode(notify_rsp, iter);
  }catch(const buffer::error &err)
  {
    ldout(m_cct, 0) << " error decoding notify rsp: "
                    << err.what() << dendl;
    return -1;                
  }  
  
  return 0;
}

void RGWInstanceObjWatcher::process_payload(uint64_t notify_id, uint64_t handle, const RGWInstObjPayload &payload, int r)
{
  if (r < 0)
  {
    bufferlist reply_bl;
    acknowledge_notify(notify_id, handle, reply_bl);
  }
  else
  {
    apply_visitor(RGWInstObjHandlePayloadVisitor(this, notify_id, handle), payload);
  }
}

void RGWInstanceObjWatcher::acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &ack_bl)
{
  ldout(m_cct, 10) << "Notify ack to: " << m_instobj->m_name << ", ack size:" << ack_bl.length() << dendl; 
  m_instobj->m_io_ctx.notify_ack(m_instobj->m_name, notify_id, handle, ack_bl);
}

C_RGWInstObjNotifyAck::C_RGWInstObjNotifyAck(RGWInstanceObjWatcher *inst_obj_watcher_, uint64_t notify_id_, uint64_t handle_) :
                                                    inst_obj_watcher(inst_obj_watcher_), notify_id(notify_id_), handle(handle_) 
{
  CephContext *cct = inst_obj_watcher->m_cct;
  ldout(cct, 0) << " C_NotifyAck start: id=" << notify_id << ", "
                << "handle=" << handle << dendl;
}

void C_RGWInstObjNotifyAck::finish(int r) 
{
  assert(r == 0);
  CephContext *cct = inst_obj_watcher->m_cct;
  ldout(cct, 0) << " C_NotifyAck finish: id=" << notify_id << ", "
                << "handle=" << handle << dendl;

  inst_obj_watcher->acknowledge_notify(notify_id, handle, out);
}

void C_RGWInstObjResponseMessage::finish(int r) 
{
#if 0
  CephContext *cct = notify_ack->inst_obj_watcher->m_cct;
  ldout(cct, 10) << this << " C_ResponseMessage: r=" << r << dendl;

  ::encode(ResponseMessage(r), notify_ack->out);
  notify_ack->complete(0);
#endif  
}

int RGWBgtScheduler::notify_active_changelog(const string& worker_name, const string& log_name, uint64_t timeout_ms)
{
  assert(RGW_ROLE_BGT_SCHEDULER == m_role);

  bufferlist bl, ack_bl;
  ::encode(RGWInstObjNotifyMsg(ActiveChangeLogPayload(log_name)), bl);
  int r = m_instobj->m_io_ctx.notify2(worker_name, bl, timeout_ms, &ack_bl);
  if (r < 0)
  {
    if (-ETIMEDOUT == r)
    {
      ldout(m_cct, 0) << " active change log(" << log_name << ")to " << worker_name << " timeout" << dendl;  
    }
    else
    {
      ldout(m_cct, 0) << " active change log(" << log_name << ")to " << worker_name << " failed: " << cpp_strerror(r) << dendl;
    }
    
  }
  else
  {
    RGWInstObjNotifyRspMsg notify_rsp;
    if (0 != get_notify_rsp(ack_bl, notify_rsp))
    {
      return -1;
    }
    
    if (0 != notify_rsp.result)
    {
      ldout(m_cct, 0) << " active change log(" << log_name << ")to " << worker_name << " failed." << dendl;
    }
    else
    {
      ldout(m_cct, 10) << " active change log(" << log_name << ")to " << worker_name << " success." << dendl;
    }
    r = notify_rsp.result;
  }

  return r;
}

int RGWBgtScheduler::update_task_entry(uint64_t task_id)
{
  int r;
  bufferlist bl;
  uint64_t size = sizeof(RGWBgtTaskEntry);
  bl.append((const char*)&batch_tasks[task_id], size);
  r = m_instobj->m_io_ctx.write(RGW_BGT_SCHEDULER_INST, bl, size, task_id*size);
  return r;
}

int RGWBgtScheduler::notify_dispatch_task(const string& worker_name, uint64_t task_id, const string& log_name, uint64_t start, uint32_t count, uint64_t timeout_ms)
{
  assert(RGW_ROLE_BGT_SCHEDULER == m_role);

  bufferlist bl, ack_bl;
  ::encode(RGWInstObjNotifyMsg(DispatchTaskPayload(task_id, log_name, start, count)), bl);
  int r = m_instobj->m_io_ctx.notify2(worker_name, bl, timeout_ms, &ack_bl);
  if (r < 0)
  {
    if (-ETIMEDOUT == r)
    {
      ldout(m_cct, 0) << " dispatch task (" << task_id << "("  << log_name 
                      << ":" << start << ":" << count << ") to " << worker_name 
                      << " timeout" << dendl;  
    }
    else
    {
      ldout(m_cct, 0) << " dispatch task (" << task_id << "("  << log_name 
                      << ":" << start << ":" << count << ") to " << worker_name 
                      << " failed:" << cpp_strerror(r) << dendl;     
    }
  }
  else
  {
    RGWInstObjNotifyRspMsg notify_rsp;
    if (0 != get_notify_rsp(ack_bl, notify_rsp))
    {
      return -1;
    }
    
    if (0 != notify_rsp.result)
    {
      ldout(m_cct, 0) << " dispatch task (" << task_id << "("  << log_name 
                      << ":" << start << ":" << count << ") to " << worker_name 
                      << " failed:" << notify_rsp.result << dendl;    
    }
    else
    {
      ldout(m_cct, 10) << " dispatch task (" << task_id << "("  << log_name 
                      << ":" << start << ":" << count << ") to " << worker_name 
                      << " success." << dendl;
      DispatchTaskRspPayload* rsp_payload = boost::get<DispatchTaskRspPayload>(&notify_rsp.payload);
      assert(NULL != rsp_payload);

      batch_tasks[task_id].dispatch_time = ceph_clock_now(0).sec();
      update_task_entry(task_id);
    }
    r = notify_rsp.result;
  }

  return r;
}

void RGWInstanceObjWatcher::handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl)
{
  RGWInstObjNotifyMsg notify_msg;
  try
  {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_msg, iter);
  }catch(const buffer::error &err)
  {
    ldout(m_cct, 0) << " error decoding rgw inst obj notification: "
		                << err.what() << dendl;  
		return;                
  }

  process_payload(notify_id, handle, notify_msg.payload, 0);
}

void RGWInstanceObjWatcher::handle_error(uint64_t handle, int err)
{
  ldout(m_cct, 0) << " rgw inst obj watch failed: " << handle << ", "
                  << cpp_strerror(err) << dendl;
  if (RGW_INSTOBJWATCH_STATE_REGISTERED == m_watch_state)
  {
    m_watch_state = RGW_INSTOBJWATCH_STATE_ERROR;
    m_instobj->m_io_ctx.unwatch2(m_watch_handle);
  }
}

bool RGWBgtScheduler::handle_payload(const WorkerRegisterPayload& payload,	C_RGWInstObjNotifyAck *ctx)
{
  bufferlist bl;
  RGWBgtWorkerInfo worker_info;
  worker_info.state = RGW_BGT_WORKER_ACTIVE;
  /* Begin added by guokexin*/
  ldout(m_cct, 10) << " ================handle_payload   ================ "<< payload.task_id << dendl;
  worker_info.idle = ((int64_t)payload.task_id == -1) ? 1 : 0;
  /* End added ??*/
  ::encode(worker_info, bl);
  map<string, bufferlist> values;
  values[payload.worker_name] = bl;
  

  int r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);
  if (r < 0)
  {
    ldout(m_cct, 0) << " set worker state failed: " << payload.worker_name << ", "
                    << cpp_strerror(r) << dendl;    
  }
  else
  {
    data_lock.Lock();
    workers[payload.worker_name].state = RGW_BGT_WORKER_ACTIVE; 
    workers[payload.worker_name].idle = worker_info.idle;
    data_lock.Unlock();
  }

  ::encode(RGWInstObjNotifyRspMsg(r, WorkerRegisterRspPayload(active_change_log)), ctx->out);
  ldout(m_cct, 10) << "ctx->out size:" << ctx->out.length() << dendl;
  
  return true;
}

bool RGWBgtScheduler::handle_payload(const TaskFinishedPayload& payload,	C_RGWInstObjNotifyAck *ctx)
{
  ldout(m_cct, 10) << " TaskFinishedPayLoad  handle_payload" << dendl;
  bufferlist bl;
  
  RGWBgtTaskEntry *task_entry;
  task_entry = &batch_tasks[payload.task_id];
  task_entry->finish_time = payload.finish_time;
  task_entry->is_finished = true;
  
  ldout(m_cct, 10) << " task " << payload.task_id << "finished, cost total time:" << task_entry->finish_time - task_entry->dispatch_time << dendl;
  int r = update_task_entry(payload.task_id);
  if (r < 0)
  {
    ldout(m_cct, 0) << " update task entry failed: " << task_entry << ", "
                    << cpp_strerror(r) << dendl;    
    task_entry->is_finished = false;
    ::encode(RGWInstObjNotifyRspMsg(-5, TaskFinishedRspPayload(payload.task_id)), ctx->out);
    return true;
  }

  data_lock.Lock();
  workers[payload.worker_name].idle = 1;
  data_lock.Unlock();
  ::encode(RGWInstObjNotifyRspMsg(0, TaskFinishedRspPayload(payload.task_id)), ctx->out);
  return true;
}

string RGWBgtScheduler::unique_change_log_id() 
{
  string s = m_store->unique_id(max_log_id.inc());
  return (RGW_BGT_CHANGELOG_PREFIX + s);
}

void RGWBgtScheduler::update_active_change_log()
{
  data_lock.Lock();
  std :: map < std :: string, RGWBgtWorkerInfo>::iterator iter = workers.begin();
  while (iter != workers.end())
  {
    RGWBgtWorkerInfo worker_info = iter->second;
    if (RGW_BGT_WORKER_ACTIVE == worker_info.state)
    {
      notify_active_changelog(iter->first, active_change_log, m_cct->_conf->rgw_bgt_notify_timeout);
    }
    iter++;
  }
  data_lock.Unlock();
}

int RGWBgtScheduler::set_active_change_log(RGWBgtLogTransInfo& log_trans_info)
{
  int r;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;
  
  ::encode(log_trans_info.pending_active_change_log, bl);
  values[RGW_BGT_ACTIVE_CHANGE_LOG_KEY] = bl;
  r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);

  return r;
}

int RGWBgtScheduler::set_change_logs(RGWBgtLogTransInfo& log_trans_info)
{
  int r;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;
  
  ::encode(change_logs[log_trans_info.active_change_log], bl);
  values[log_trans_info.active_change_log] = bl;
  r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);

  return r;
}

int RGWBgtScheduler::set_batch_task_info(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;
  
  ::encode(batch_task_info, bl);
  values[RGW_BGT_BATCH_TASK_META_KEY] = bl;
  r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);

  return r;
}

int RGWBgtScheduler::get_batch_task_info(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;
  std :: set < std :: string >keys;
  std :: map < std :: string,bufferlist > vals;
  keys.insert(RGW_BGT_BATCH_TASK_META_KEY);
  r = m_instobj->m_io_ctx.omap_get_vals_by_keys(RGW_BGT_SCHEDULER_INST, keys, &vals);
  if (0 == r)
  {
    if (0 == vals[RGW_BGT_BATCH_TASK_META_KEY].length())
    {
      batch_task_info.stage = RGW_BGT_BATCH_TASK_FINISH;
      ldout(m_cct, 0) << "thers is no batch task info" << dendl;
    }
    else
    {
      bufferlist::iterator iter = vals[RGW_BGT_BATCH_TASK_META_KEY].begin();
      ::decode(batch_task_info, iter);
      ldout(m_cct, 10) << "load batch task info success:" << batch_task_info.stage << dendl;
    }
  }
  else
  {
    ldout(m_cct, 0) << "load batch task info failed:" << cpp_strerror(r) << dendl;
  }

  return r;
}

void RGWBgtScheduler::clear_task_entry(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;

  batch_tasks.erase(batch_tasks.begin(),batch_tasks.end());
  
  r = m_instobj->m_io_ctx.trunc(RGW_BGT_SCHEDULER_INST, 0);
  if (0 == r)
  {
    batch_task_info.stage = RGW_BGT_BATCH_TASK_FINISH;
    set_batch_task_info(batch_task_info);
    ldout(m_cct, 10) << "clear task entry success" << dendl;
  }
  else
  {
    ldout(m_cct, 0) << "clear task entry failed:" << cpp_strerror(r) << dendl;
  }
}

void RGWBgtScheduler::rm_log_entry(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;
  std :: set < std :: string >keys;

  keys.insert(batch_task_info.log_name);
  r = m_instobj->m_io_ctx.omap_rm_keys(RGW_BGT_SCHEDULER_INST, keys);
  if (0 == r)
  {
    std :: map < std :: string, RGWBgtChangeLogInfo>::iterator iter = change_logs.find(batch_task_info.log_name);
    if (iter != change_logs.end())
    {
      change_logs.erase(iter);
    }
    batch_task_info.stage = RGW_BGT_BATCH_TASK_WAIT_CLEAR_TASK_ENTRY;
    set_batch_task_info(batch_task_info);
    ldout(m_cct, 10) << "Remove log entry success:" << batch_task_info.log_name << dendl;
  }
  else
  {
    ldout(m_cct, 0) << "Remove log entry failed:" << cpp_strerror(r) << dendl;
  }
}

void RGWBgtScheduler::rm_change_log(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;

  //r = m_instobj->m_striper.remove(batch_task_info.log_name);
  r = m_instobj->m_io_ctx.remove(batch_task_info.log_name);
  if (0 == r)
  {
    batch_task_info.stage = RGW_BGT_BATCH_TASK_WAIT_RM_LOG_ENTRY;
    set_batch_task_info(batch_task_info);
    ldout(m_cct, 10) << "Remove log file success:" << batch_task_info.log_name << dendl;
  }
  else
  {
    ldout(m_cct, 0) << "Remove change log failed:" << cpp_strerror(r) << dendl;
  }
}

/* modified by guokexin */
void RGWBgtScheduler::dispatch_task(RGWBgtBatchTaskInfo& batch_task_info)
{
  ldout(m_cct, 10) << "=== dispatch_task 01===  next_task task_cnt"<< batch_task_info.next_task << batch_task_info.task_cnt <<dendl;
  if (batch_task_info.next_task >= batch_task_info.task_cnt)
  {
    for (uint64_t i = 0; i < batch_task_info.task_cnt; i++)
    {
      RGWBgtTaskEntry* task_entry = &batch_tasks[i];
      if (!task_entry->is_finished)
      {
        return;
      }
    }

    ldout(m_cct, 10) << "enter RGW_BGT_BATCH_TASK_WAIT_RM_LOG" << dendl;
    batch_task_info.stage = RGW_BGT_BATCH_TASK_WAIT_RM_LOG;
    set_batch_task_info(batch_task_info);

    return;
  }
  
  ldout(m_cct, 10) << "=== dispatch_task 02===" << dendl;
  RGWBgtWorkerInfo* worker_info;
  data_lock.Lock();
  std :: map < std :: string, RGWBgtWorkerInfo>::iterator iter = workers.begin();
  while (iter != workers.end() && batch_task_info.next_task < batch_task_info.task_cnt)
  {
    worker_info = &(iter->second);
    ldout(m_cct, 10) << " worker_info->state " <<  worker_info->state << " "<<" work_info->idle "<< worker_info->idle << dendl; 
    if (RGW_BGT_WORKER_ACTIVE == worker_info->state && worker_info->idle)
    { 
      RGWBgtTaskEntry* task_entry = &batch_tasks[batch_task_info.next_task];
      int r;
      
      ldout(m_cct, 10) << "=== dispatch_task 03===" << dendl;
      r = notify_dispatch_task(iter->first, batch_task_info.next_task, batch_task_info.log_name, task_entry->start, task_entry->count, m_cct->_conf->rgw_bgt_notify_timeout);
      if (0 == r)
      {
        worker_info->idle = 0;
        batch_task_info.next_task++;
        set_batch_task_info(batch_task_info);
      }
      ldout(m_cct, 10) << "=== dispatch_task 04===" << dendl;
    }
    iter++;
  }
  data_lock.Unlock();
}

void RGWBgtScheduler::mk_task_entry_from_buf(bufferlist& bl, uint64_t rs_cnt, uint64_t& next_start, 
                                                   uint64_t& log_index, uint64_t& merge_size, 
                                                   uint64_t& task_id)
{
  RGWChangeLogEntry* log_entry;
  char* buffer;
  RGWBgtTaskEntry task_entry;
  
  buffer = bl.c_str();
  for (uint64_t j = 0; j < rs_cnt; j++)
  {
    log_entry = (RGWChangeLogEntry*)&buffer[j*RGW_CHANGE_LOG_ENTRY_SIZE];
    merge_size += log_entry->size;
    log_index++;
  
    if (merge_size >= (m_cct->_conf->rgw_bgt_merged_obj_size<<20))
    {
      task_entry.start = next_start;
      task_entry.count = log_index - next_start;
  
      batch_tasks[task_id] = task_entry;
      task_id++;
      next_start += task_entry.count;  
      merge_size= 0;
    }
  }
}

void RGWBgtScheduler::set_task_entries(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;
  uint64_t task_cnt = batch_tasks.size();
  bufferlist bl_entries;
  std :: map < uint64_t, RGWBgtTaskEntry>::iterator iter = batch_tasks.begin();

  while (iter != batch_tasks.end())
  {
    bl_entries.append((char*)&iter->second, sizeof(RGWBgtTaskEntry));
    iter++;
  }

  librados::ObjectWriteOperation writeOp;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;

  batch_task_info.task_cnt = task_cnt;
  batch_task_info.next_task = 0;
  batch_task_info.stage = RGW_BGT_BATCH_TASK_WAIT_DISPATCH;
  ::encode(batch_task_info, bl);
  values[RGW_BGT_BATCH_TASK_META_KEY] = bl;

  writeOp.omap_set(values);
  writeOp.write_full(bl_entries);
  
  r = m_instobj->m_io_ctx.operate(RGW_BGT_SCHEDULER_INST, &writeOp);
  if (r < 0)
  {
    ldout(m_cct, 0) << "m_io_ctx.operate:" << cpp_strerror(r) << dendl;
  }
}

void RGWBgtScheduler::create_task(RGWBgtBatchTaskInfo& batch_task_info)
{
  int r;
  time_t t;
  uint64_t size;
  uint64_t log_entry_cnt;

  //r = m_instobj->m_striper.stat(batch_task_info.log_name, &size, &t);
  ldout(m_cct, 10) << "obj_name" << batch_task_info.log_name <<dendl;
  r = m_instobj->m_io_ctx.stat(batch_task_info.log_name, &size, &t);
  if (0 != r)
  {
    ldout(m_cct, 0) << "stat failed:" << cpp_strerror(r) << dendl;
    return;
  }

  log_entry_cnt = size / RGW_CHANGE_LOG_ENTRY_SIZE;
  uint32_t step = 4096;
  uint32_t cnt = log_entry_cnt / step;
  uint32_t leave = log_entry_cnt % step;

  uint64_t next_start = 0;
  uint64_t log_index = 0;
  uint64_t merge_size = 0;
  uint64_t task_id = 0;
  uint64_t step_size = RGW_CHANGE_LOG_ENTRY_SIZE*step;
  batch_task_info.next_task = 0;
  for (uint32_t i = 0; i < cnt; i++)
  {
    bufferlist bl;
    //r = m_instobj->m_striper.read(batch_task_info.log_name, &bl, step_size, i*step_size);
    r = m_instobj->m_io_ctx.read(batch_task_info.log_name, bl, step_size, i*step_size);
    if (r < 0)
    {
      ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
      return;
    }
    mk_task_entry_from_buf(bl, step, next_start, log_index, merge_size, task_id);
  }

  bufferlist bl;
  //r = m_instobj->m_striper.read(batch_task_info.log_name, &bl, RGW_CHANGE_LOG_ENTRY_SIZE*leave, cnt*step_size);
  r = m_instobj->m_io_ctx.read(batch_task_info.log_name, bl, RGW_CHANGE_LOG_ENTRY_SIZE*leave, cnt*step_size);
  if (r < 0)
  {
    ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
    return;
  }
  mk_task_entry_from_buf(bl, leave, next_start, log_index, merge_size, task_id);

  if (merge_size)
  {
    if (task_id >= 1)
    {
      batch_tasks[task_id-1].count = log_index - batch_tasks[task_id-1].start;
    }
    else
    {
      batch_tasks[0].start = 0;
      batch_tasks[0].count = log_index;
    }
  }
  set_task_entries(batch_task_info);
}
//added by guokexin , append log to out  
void RGWBgtScheduler::check_batch_task()
{
  RGWBgtBatchTaskInfo batch_task_info;
  int r;
  r = get_batch_task_info(batch_task_info);
  if (0 != r)
  {
    return;
  }

  switch (batch_task_info.stage)
  {
    case RGW_BGT_BATCH_TASK_FINISH:
    {
      ldout(m_cct, 10) << "=== RGW_BGT_BATCH_TASK_FINISH" << dendl;
      std :: map < std :: string, RGWBgtChangeLogInfo>::iterator iter = change_logs.begin();
      iter++;
      if (iter != change_logs.end())
      {
        batch_task_info.log_name = iter->first;
        batch_task_info.stage = RGW_BGT_BATCH_TASK_WAIT_CREATE;
        set_batch_task_info(batch_task_info);
      }
    }
    break;

    case RGW_BGT_BATCH_TASK_WAIT_CLEAR_TASK_ENTRY:
    {
      ldout(m_cct, 10) << "RGW_BGT_BATCH_TASK_WAIT_CLEAR_TASK_ENTRY" << dendl;
      clear_task_entry(batch_task_info);
    }
    break;

    case RGW_BGT_BATCH_TASK_WAIT_RM_LOG_ENTRY:
    {
      ldout(m_cct, 10) << "RGW_BGT_BATCH_TASK_WAIT_RM_LOG_ENTRY" << dendl;
      rm_log_entry(batch_task_info);
    }
    break;

    case RGW_BGT_BATCH_TASK_WAIT_RM_LOG:
    {
      ldout(m_cct, 10) << "RGW_BGT_BATCH_TASK_WAIT_RM_LOG" << dendl;
      rm_change_log(batch_task_info);
    }
    break;

    case RGW_BGT_BATCH_TASK_WAIT_DISPATCH:
    {
      ldout(m_cct, 10) << " RGW_BGT_BATCH_TASK_WAIT_DISPATCH " << dendl;
      dispatch_task(batch_task_info);
    }
    break;

    case RGW_BGT_BATCH_TASK_WAIT_CREATE:
    {
      ldout(m_cct, 10) << "RGW_BGT_BATCH_TASK_WAIT_CREATE" <<dendl;
      create_task(batch_task_info);
    }
    break;

    default:
    {
      assert(0);
    }
    break;
  }
}

int RGWBgtScheduler::get_log_trans_info(RGWBgtLogTransInfo& log_trans_info)
{
  int r;
  std :: set < std :: string >keys;
  std :: map < std :: string,bufferlist > vals;
  keys.insert(RGW_BGT_LOG_TRANS_META_KEY);
  r = m_instobj->m_io_ctx.omap_get_vals_by_keys(RGW_BGT_SCHEDULER_INST, keys, &vals);
  if (0 == r)
  {
    if (0 == vals[RGW_BGT_LOG_TRANS_META_KEY].length())
    {
      log_trans_info.stage = RGW_BGT_LOG_TRANS_FINISH;
      /* Begin comment by guokexin */
      // ldout(m_cct, 0) << "there is no log trans info" << dendl;
      /* End comment */
    }
    else
    {
      bufferlist::iterator iter = vals[RGW_BGT_LOG_TRANS_META_KEY].begin();
      ::decode(log_trans_info, iter);
      /* Begin comment by guokexin */
      // ldout(m_cct, 0) << "load log trans info success:" << log_trans_info.stage << dendl;
      /* End comment */
    }
  }
  else
  {
    ldout(m_cct, 0) << "load log trans info failed:" << cpp_strerror(r) << dendl;
  }

  return r;  
}

int RGWBgtScheduler::set_log_trans_info(RGWBgtLogTransInfo& log_trans_info)
{
  int r;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;
  
  ::encode(log_trans_info, bl);
  values[RGW_BGT_LOG_TRANS_META_KEY] = bl;
  r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);

  return r;
}

void RGWBgtScheduler::check_log_trans()
{
  static utime_t t1 = ceph_clock_now(0);
  bool need_retry_set_log_trans_info = false;
  
  RGWBgtLogTransInfo log_trans_info;
  int r;
  
  r = get_log_trans_info(log_trans_info);
  if (0 != r)
  {
    return;
  }

  switch (log_trans_info.stage)
  {
    case RGW_BGT_LOG_TRANS_START:
    {
      log_trans_info.active_change_log = active_change_log;
      log_trans_info.pending_active_change_log = unique_change_log_id();
      log_trans_info.stage = RGW_BGT_LOG_TRANS_WAIT_CREATE_LOG_OBJ;
      set_log_trans_info(log_trans_info);
    }
    break;

    case RGW_BGT_LOG_TRANS_WAIT_CREATE_LOG_OBJ:
    {
      int r;
      
      //r = m_instobj->m_striper.create(log_trans_info.pending_active_change_log);
      //r = m_instobj->m_io_ctx.trunc(log_trans_info.pending_active_change_log, 0);
      r = m_instobj->m_io_ctx.create(log_trans_info.pending_active_change_log, false);
      if (0 == r)
      {
        ldout(m_cct, 0) << "create new change log success:" << log_trans_info.pending_active_change_log << dendl;
        log_trans_info.stage = RGW_BGT_LOG_TRANS_WAIT_ADD_TO_INACTIVE;
        set_log_trans_info(log_trans_info);
      }
    }
    break;

    case RGW_BGT_LOG_TRANS_WAIT_ADD_TO_INACTIVE:
    {
        RGWBgtChangeLogInfo log_info;
        int r;
        change_logs[active_change_log] = log_info; 

        r = set_change_logs(log_trans_info);
        if (0 == r)
        {
          log_trans_info.stage = RGW_BGT_LOG_TRANS_WAIT_SET_ACTIVE;
          set_log_trans_info(log_trans_info);
        }
    }
    break;

    case RGW_BGT_LOG_TRANS_WAIT_SET_ACTIVE:
    {
      int r = set_active_change_log(log_trans_info);
      if (0 == r)
      {
        active_change_log = log_trans_info.pending_active_change_log;
        log_trans_info.stage = RGW_BGT_LOG_TRANS_WAIT_NOTIFY_WORKER;
        set_log_trans_info(log_trans_info);
      }      
    }
    break;

    case RGW_BGT_LOG_TRANS_WAIT_NOTIFY_WORKER:
    {
      update_active_change_log();
      log_trans_info.stage = RGW_BGT_LOG_TRANS_FINISH;
      set_log_trans_info(log_trans_info);
    }

    case RGW_BGT_LOG_TRANS_FINISH:
    {
      utime_t t2 = ceph_clock_now(0);
      if (t2.sec() - t1.sec() >= m_cct->_conf->rgw_bgt_change_log_check_interval)
      {
        t1 = t2;

        int r;
        time_t t;
        uint64_t size;
        
        //guokexin
        //r = m_instobj->m_striper.stat(log_trans_info.pending_active_change_log, &size, &t);
        r = m_instobj->m_io_ctx.stat(log_trans_info.pending_active_change_log, &size, &t);
        if ((0 == r) &&
             ((size /= (1<<20)) >= m_cct->_conf->rgw_bgt_change_log_max_size))
        //if ((0 == r) &&
        //     ((size) >= m_cct->_conf->rgw_bgt_change_log_max_size))
        {
          log_trans_info.stage = RGW_BGT_LOG_TRANS_START;
          r = set_log_trans_info(log_trans_info);
          need_retry_set_log_trans_info = (0 != r);
        }
      }
      else if (need_retry_set_log_trans_info)
      {
        log_trans_info.stage = RGW_BGT_LOG_TRANS_START;
        r = set_log_trans_info(log_trans_info);
        need_retry_set_log_trans_info = (0 != r);
      }
    }
    break;
    
    default:
    {
    }
    break;
  }
}

void* RGWBgtScheduler::entry()
{
  while (!stopping)
  {
    uint32_t sec = m_cct->_conf->rgw_bgt_tick_interval/1000;
    uint32_t nsec = (m_cct->_conf->rgw_bgt_tick_interval % 1000)*1000*1000; 

    check_log_trans();
    check_batch_task();

    lock.Lock();
    cond.WaitInterval(NULL, lock, utime_t(sec, nsec));
    lock.Unlock();
  }

  return NULL;
}
void RGWBgtScheduler::start()
{
  {
    Mutex::Locker l(lock);
    stopping = false;
  }
  create();
}
void RGWBgtScheduler::stop()
{
  {
    Mutex::Locker l(lock);
    stopping = true;
    cond.Signal();
  }

  join();
}

void RGWBgtScheduler::load_change_logs()
{
  int r;
  std :: map < std :: string,bufferlist > logs;
  r = m_instobj->m_io_ctx.omap_get_vals(RGW_BGT_SCHEDULER_INST, "", RGW_BGT_CHANGELOG_PREFIX, 1000, &logs);
  if (0 == r)
  {
    std :: map < std :: string,bufferlist >::iterator ilog = logs.begin();
    ldout(m_cct, 0) << "change log info: " << dendl;
    while (ilog != logs.end())
    {
      bufferlist::iterator iter = ilog->second.begin();
      RGWBgtChangeLogInfo log_info;
      ::decode(log_info, iter);
      change_logs[ilog->first] = log_info;
      ldout(m_cct, 0) << ilog->first << ":" << log_info.state << dendl;
      ilog++;
    }
  }
}

void RGWBgtScheduler::load_active_change_log()
{
  int r;
  RGWBgtLogTransInfo log_trans_info;
  
  r = get_log_trans_info(log_trans_info);
  assert(0==r);

  if (RGW_BGT_LOG_TRANS_FINISH == log_trans_info.stage)
  {
    if (0 == log_trans_info.pending_active_change_log.length())
    {
      bufferlist bl;
      std :: map < std :: string,bufferlist > values;
      active_change_log = unique_change_log_id();
      ::encode(active_change_log, bl);
      values[RGW_BGT_ACTIVE_CHANGE_LOG_KEY] = bl;
      r = m_instobj->m_io_ctx.omap_set(RGW_BGT_SCHEDULER_INST, values);
      if (0 == r)
      {
        //r = m_instobj->m_striper.create(active_change_log);
        //r = m_instobj->m_io_ctx.trunc(active_change_log, 0);
        r = m_instobj->m_io_ctx.create(active_change_log, true);
        if (0 == r)
        {
          log_trans_info.active_change_log = active_change_log;
          log_trans_info.pending_active_change_log = active_change_log;
          log_trans_info.stage = RGW_BGT_LOG_TRANS_FINISH;
          r = set_log_trans_info(log_trans_info);
        }
      }
      assert(0 == r);
      ldout(m_cct, 0) << "create new change log success:" << active_change_log << dendl;
    }
    else
    {
      active_change_log = log_trans_info.pending_active_change_log;
      ldout(m_cct, 0) << "current change log :" << active_change_log << dendl;
    }
  }
  else
  {
    active_change_log = log_trans_info.active_change_log;
    ldout(m_cct, 0) << "current change log :" << active_change_log << dendl;
  }
}

void RGWBgtScheduler::load_workers()
{
  int r;
  std :: map < std :: string,bufferlist > wks;
  r = m_instobj->m_io_ctx.omap_get_vals(RGW_BGT_SCHEDULER_INST, "", RGW_BGT_WORKER_INST_PREFIX, 1000, &wks);
  if (0 == r)
  {
    std :: map < std :: string,bufferlist >::iterator iwk = wks.begin();
    ldout(m_cct, 0) << "workers info: " << dendl;
    while (iwk != wks.end())
    {
      bufferlist::iterator iter = iwk->second.begin();
      RGWBgtWorkerInfo worker_info;
      ::decode(worker_info, iter);
      data_lock.Lock();
      workers[iwk->first] = worker_info;
      data_lock.Unlock();
      ldout(m_cct, 0) << iwk->first << ":" << worker_info.state << dendl;
      iwk++;
    }
  }
}

void RGWBgtScheduler::load_tasks()
{
  int r;
  uint64_t size;
  time_t t;
  RGWBgtBatchTaskInfo batch_task_info; 

  r = get_batch_task_info(batch_task_info);
  if (0 != r)
  {
    ldout(m_cct, 0) << "get batch task info failed" << dendl;
    assert(0);
  }

  if (RGW_BGT_BATCH_TASK_WAIT_CREATE == batch_task_info.stage ||
      RGW_BGT_BATCH_TASK_FINISH == batch_task_info.stage)
  {
    ldout(m_cct, 0) << "no batch task info" << dendl;
    return;
  }
  
  r = m_instobj->m_io_ctx.stat(RGW_BGT_SCHEDULER_INST, &size, &t);
  if (0 != r)
  {
    ldout(m_cct, 0) << "stat batch task info size failed" << dendl;
    assert(0);
    return;
  }

  if (batch_task_info.task_cnt*sizeof(RGWBgtTaskEntry) != size)
  {
    ldout(m_cct, 0) << "the batch task entry is incomplete:" << size 
                    << "," << batch_task_info.task_cnt << "," << sizeof(RGWBgtTaskEntry) << dendl;
    assert(0);
    return;
  }
  
  RGWBgtTaskEntry* task_entries;
  bufferlist bl;
  
  r = m_instobj->m_io_ctx.read(RGW_BGT_SCHEDULER_INST, bl, size, 0);
  if (r > 0)
  {
    task_entries = (RGWBgtTaskEntry*)bl.c_str();
    for(uint64_t i = 0; i < batch_task_info.task_cnt; i++)
    {
      batch_tasks[i] = task_entries[i];
    }  
  }
  else
  {
    ldout(m_cct, 0) << "read task entry failed:" << cpp_strerror(r) << dendl;
    assert(0);
  }
}

int get_obj_index_meta(librados::IoCtx& io_ctx, CephContext *m_cct, string bi_oid, string oid, rgw_bucket_dir_entry& bi_entry)
{
  int r;
  std :: set < std :: string >keys;
  std :: map < std :: string,bufferlist > vals;
  keys.insert(oid);
  r = io_ctx.omap_get_vals_by_keys(bi_oid, keys, &vals);
  if (0 == r)
  {
    ldout(m_cct, 10) << "bi_entry len:" << vals[oid].length() 
                    << ", " << bi_oid << ", " << oid << dendl;
    
    bufferlist::iterator iter = vals[oid].begin();
    ::decode(bi_entry, iter);
    ldout(m_cct, 10) << "bi_entry len:" << vals[oid].length() << dendl;
    ldout(m_cct, 10) << "==============obj index info================" << dendl; 
    ldout(m_cct, 10) << "name=" << bi_entry.key.name << dendl;
    ldout(m_cct, 10) << "epoch=" << bi_entry.ver.epoch << dendl;
    ldout(m_cct, 10) << "locator=" << bi_entry.locator << dendl;

    ldout(m_cct, 10) << "category=" << bi_entry.meta.category << dendl;
    ldout(m_cct, 10) << "size=" << bi_entry.meta.size << dendl;
    ldout(m_cct, 10) << "etag=" << bi_entry.meta.etag << dendl;
    ldout(m_cct, 10) << "owner=" << bi_entry.meta.owner << dendl;
    ldout(m_cct, 10) << "display name=" << bi_entry.meta.owner_display_name << dendl;
    ldout(m_cct, 10) << "content type=" << bi_entry.meta.content_type << dendl;
    ldout(m_cct, 10) << "accounted size=" << bi_entry.meta.accounted_size << dendl;

    ldout(m_cct, 10) << "is_merged=" << bi_entry.meta.is_merged << dendl;
    ldout(m_cct, 10) << "data_pool=" << bi_entry.meta.data_pool << dendl;
    ldout(m_cct, 10) << "data_oid=" << bi_entry.meta.data_oid << dendl;
    ldout(m_cct, 10) << "data_offset=" << bi_entry.meta.data_offset << dendl;
    ldout(m_cct, 10) << "data_size=" << bi_entry.meta.data_size << dendl;
  }
  else
  {
    ldout(m_cct, 0) << __func__ << "error get object index meta " << ": "
	                  << cpp_strerror(r) << dendl;     
  }  

  return r;
}

RGWBgtScheduler::RGWBgtScheduler(RGWInstanceObj* instobj, RGWRados* store, CephContext* cct) :
                                       RGWInstanceObjWatcher(instobj, RGW_ROLE_BGT_SCHEDULER, store, cct),
                                       stopping(true),
                                       lock(rgw_unique_lock_name("RGWBgtScheduler::lock", this), false, true, false, cct),
                                       data_lock(rgw_unique_lock_name("RGWBgtScheduler::data_lock", this), false, true, false, cct),
                                       max_log_id(0), 
                                       active_change_log("")
{
#if 0
  string bucket("Demon");
  string oid("test2.jpg");
  string bi_oid(".dir.default.214137.1.1");

  RGWBucketInfo bucket_info;
  RGWObjectCtx obj_ctx(store);
  time_t mtime; 
  int r;

  r = store->get_bucket_info(obj_ctx, "", bucket, bucket_info, &mtime);
  if (r < 0)
  {
    ldout(m_cct, 0) << "get bucket info failed:" << cpp_strerror(r) << dendl;
    return;  
  }

  ldout(m_cct, 0) << "==============bucket info================" << dendl;
  ldout(m_cct, 0) << "name=" << bucket_info.bucket.name << dendl;
  ldout(m_cct, 0) << "data pool=" << bucket_info.bucket.data_pool << dendl;
  ldout(m_cct, 0) << "data extra pool=" << bucket_info.bucket.data_extra_pool << dendl;
  ldout(m_cct, 0) << "index pool=" << bucket_info.bucket.index_pool << dendl;
  ldout(m_cct, 0) << "marker=" << bucket_info.bucket.marker << dendl;
  ldout(m_cct, 0) << "bucket id=" << bucket_info.bucket.bucket_id << dendl;
  ldout(m_cct, 0) << "bucket instance oid=" << bucket_info.bucket.oid << dendl;
#if 0
  rgw_obj obj(bucket_info.bucket, oid);
  rgw_obj_key key;
  obj.get_index_key(&key);
{
  JSONFormatter jf(true);
  jf.open_object_section("obj info");
  obj.dump(&jf);
  jf.dump_string("key.name=", key.name);
  jf.close_section();
  jf.flush(cout);
}  
#endif
  


//#if 1  
  librados::IoCtx io_ctx;
  librados::Rados *rad = store->get_rados_handle();
  r = rad->ioctx_create(bucket_info.bucket.index_pool.c_str(), io_ctx);
  if (r < 0)
  {
    ldout(m_cct, 0) << __func__ << "error opening pool " << bucket_info.bucket.index_pool << ": "
	                  << cpp_strerror(r) << dendl;  
    return;
  }

  rgw_bucket_dir_entry bi_entry;
  r = get_obj_index_meta(io_ctx, m_cct, bi_oid, oid, bi_entry);
  if (0 == r)
  {
    bi_entry.meta.is_merged = true;
    bi_entry.meta.data_pool = "xsky_bgt";
    bi_entry.meta.data_oid = "xsky_merged";
    bi_entry.meta.data_offset = 0;
    bi_entry.meta.data_size = 8024990;

    bufferlist bl;
    std :: map < std :: string,bufferlist > vals;
    ::encode(bi_entry, bl);
    vals[oid] = bl;
    r = io_ctx.omap_set(bi_oid, vals);
    if (0 == r)
    {
      ldout(m_cct, 0) << "update object index success:" << dendl;
      rgw_bucket_dir_entry bi_entry2;
      get_obj_index_meta(io_ctx, m_cct, bi_oid, oid, bi_entry2);
    }
    else
    {
      ldout(m_cct, 0) << "update object index failed:" << cpp_strerror(r) << dendl;
    }
  }
  else
  {
    ldout(m_cct, 0) << __func__ << "error get object index meta " << bucket_info.bucket.index_pool << ": "
	                  << cpp_strerror(r) << dendl;     
  }
#endif  
  load_active_change_log();
  load_change_logs();
  load_workers();
  load_tasks();

  start();
}

RGWBgtWorker::RGWBgtWorker(RGWInstanceObj* instobj, RGWRados* store, CephContext* cct) :
                                 RGWInstanceObjWatcher(instobj, RGW_ROLE_BGT_WORKER, store, cct),
                                 stopping(true),
                                 lock(rgw_unique_lock_name("RGWBgtWorker::lock", this), false, true, false, cct),
                                 max_sfm_id(0),
                                 active_change_log(""),
                                 update_change_log_lock(rgw_unique_lock_name("RGWBgtWorker::update_lock", this)),
                                 merge_stage(RGW_BGT_TASK_SFM_START), 
                                 m_archive_task(cct, store, this)
{
  load_task();
  
  int r = notify_register(m_cct->_conf->rgw_bgt_notify_timeout);
  /*Begin added by guokexin*/
  if(r !=0 ){
    ldout(m_cct, 10) << "worker register fail" << dendl; 
    exit(0);
  }
  /* End add */
  assert(0 == r);
  
  m_archive_task.start();
  start();  
}

int RGWBgtWorker::notify_register(uint64_t timeout_ms)
{
  assert(RGW_ROLE_BGT_WORKER == m_role);

  bufferlist bl, ack_bl;
  int r;
  uint64_t task_id;
  RGWBgtTaskInfo task_info;
  r = get_task_info(task_info);
  assert(0 == r);
  task_id = (RGW_BGT_TASK_FINISH == task_info.stage) ? -1 : task_info.task_id;
  ::encode(RGWInstObjNotifyMsg(WorkerRegisterPayload(this->m_instobj->m_name, task_id)), bl);
  r = m_instobj->m_io_ctx.notify2(RGW_BGT_SCHEDULER_INST, bl, timeout_ms, &ack_bl);
  if (r < 0)
  {
    if (-ETIMEDOUT == r)
    {
      ldout(m_cct, 0) << " worker register timeout" << dendl;  
    }
    else
    {
      ldout(m_cct, 0) << " fail to register: " << cpp_strerror(r) << dendl;
    }
    
  }
  else
  {
    RGWInstObjNotifyRspMsg notify_rsp;
    if (0 != get_notify_rsp(ack_bl, notify_rsp))
    {
      return -1;
    }

    if (0 != notify_rsp.result)
    {
      ldout(m_cct, 0) << m_instobj->m_name << " login failed." << dendl;
    }
    else
    {
      m_state = RGW_BGT_WORKER_ACTIVE;
      {
        WorkerRegisterRspPayload* rsp_payload = boost::get<WorkerRegisterRspPayload>(&notify_rsp.payload);
        assert(NULL != rsp_payload);
        RWLock::WLocker l(update_change_log_lock);
        active_change_log = rsp_payload->active_change_log;
      }
      ldout(m_cct, 0) << m_instobj->m_name << " login success, active change log:" << active_change_log << dendl;
    }
    r = notify_rsp.result;
  }

  return r;
}

int RGWBgtWorker::notify_task_finished(uint64_t task_id, uint64_t timeout_ms)
{
  assert(RGW_ROLE_BGT_WORKER == m_role);

  bufferlist bl, ack_bl;
  utime_t u = ceph_clock_now(0);
  ::encode(RGWInstObjNotifyMsg(TaskFinishedPayload(task_id, u.sec(),m_instobj->m_name)), bl);
  int r = m_instobj->m_io_ctx.notify2(RGW_BGT_SCHEDULER_INST, bl, timeout_ms, &ack_bl);
  if (r < 0)
  {
    if (-ETIMEDOUT == r)
    {
      ldout(m_cct, 0) << " notify task finish (" << task_id << ")" << " timeout" << dendl;  
    }
    else
    {
      ldout(m_cct, 0) << " notify task finish (" << task_id << ")" << " failed:" << cpp_strerror(r) << dendl;
    }
  }
  else
  {
    RGWInstObjNotifyRspMsg notify_rsp;
    if (0 != get_notify_rsp(ack_bl, notify_rsp))
    {
      return -1;
    }
    
    if (0 != notify_rsp.result)
    {
      ldout(m_cct, 0) << " notify task finish (" << task_id << ")" << " failed:" << notify_rsp.result << dendl;  
    }
    else
    {
      ldout(m_cct, 0) << " notify task finish (" << task_id << ")" << " success" << dendl;
    }
    r = notify_rsp.result;
  }

  return r;
}

bool RGWBgtWorker::handle_payload(const ActiveChangeLogPayload& payload,	C_RGWInstObjNotifyAck *ctx)
{
  {
    ldout(m_cct, 0) << "update active change log: " << active_change_log << "->" << payload.log_name << dendl;
    RWLock::WLocker l(update_change_log_lock);
    active_change_log = payload.log_name;
  }  
  ::encode(RGWInstObjNotifyRspMsg(0, WorkerRegisterRspPayload(this->m_instobj->m_name)), ctx->out);
  return true;
}

bool RGWBgtWorker::handle_payload(const DispatchTaskPayload& payload,	C_RGWInstObjNotifyAck *ctx)
{
  int r;
  RGWBgtTaskInfo task_info;
  
  r = get_task_info(task_info);
  if (0 != r)
  {
    ldout(m_cct, 0) << " get task meta info failed: " << cpp_strerror(r) << dendl;
    ::encode(RGWInstObjNotifyRspMsg(-2, DispatchTaskRspPayload(0,"")), ctx->out);
    return true;
  }
  
  if (RGW_BGT_TASK_FINISH != task_info.stage)
  {
    ldout(m_cct, 0) << " task has not finished: " << task_info.task_id << "," << task_info.stage << dendl;
    ::encode(RGWInstObjNotifyRspMsg(-3, DispatchTaskRspPayload(0,"")), ctx->out);

    return true;
  }  
  task_info.dst_pool = m_cct->_conf->rgw_bgt_archive_pool;
  task_info.dst_file = unique_sfm_oid();
  task_info.task_id = payload.task_id;
  task_info.log_name = payload.log_name;
  task_info.start = payload.start;
  task_info.count = payload.count;
  task_info.stage = RGW_BGT_TASK_WAIT_CREATE_SFMOBJ;

  r = set_task_info(task_info);
  if (r < 0)
  {
    ldout(m_cct, 0) << " set task info failed: " << task_info.task_id << cpp_strerror(r) << dendl;
    ::encode(RGWInstObjNotifyRspMsg(-4, DispatchTaskRspPayload(0,"")), ctx->out);  
    return true;
  }

  ldout(m_cct, 10) << " set task info success: " << task_info.task_id << dendl;  
  ::encode(RGWInstObjNotifyRspMsg(0, DispatchTaskRspPayload(payload.task_id, m_instobj->m_name)), ctx->out);
  return true;
}

string RGWBgtWorker::unique_sfm_oid() 
{
  string s = m_store->unique_id(max_sfm_id.inc());
  return (RGW_BGT_MERGEFILE_PREFIX + s);
}

int RGWBgtWorker::write_change_log(RGWChangeLogEntry& log_entry)
{
  int r;
  string cur_active_change_log;
  bufferlist bl;
  bl.append((char*)&log_entry, RGW_CHANGE_LOG_ENTRY_SIZE); 

  {
    RWLock::RLocker l(update_change_log_lock);
    cur_active_change_log = active_change_log;
  }
  
  //r = m_instobj->m_striper.append(active_change_log, bl, RGW_CHANGE_LOG_ENTRY_SIZE);
  r = m_instobj->m_io_ctx.append(cur_active_change_log, bl, RGW_CHANGE_LOG_ENTRY_SIZE);
  if (r >= 0)
  {
    ldout(m_cct, 10) << "append change log success" << dendl;
  }
  else
  {
    ldout(m_cct, 0) << "append change log failed(" << log_entry.bucket 
                    << "/" << log_entry.oid << ") : " << cpp_strerror(r) << dendl;
  }
  return r;
}

int RGWBgtWorker::create_sfm_obj(RGWBgtTaskInfo& task_info)
{
#if 0
  if (NULL != m_sfm_striper)
  {
    delete m_sfm_striper;
    m_sfm_io_ctx = NULL;
  }
#endif  
  if (NULL != m_sfm_io_ctx)
  {
    delete m_sfm_io_ctx;
    m_sfm_io_ctx = NULL;
  }
#if 0
  m_sfm_striper = new libradosstriper::RadosStriper();
  assert(NULL != m_sfm_striper);
#endif
  m_sfm_io_ctx = new librados::IoCtx();
  assert(NULL != m_sfm_io_ctx);
  
  bufferlist bl;
  librados::Rados *rad = m_store->get_rados_handle();
  int r = rad->ioctx_create(task_info.dst_pool.c_str(), *m_sfm_io_ctx);
  if (r < 0)
  {
    ldout(m_cct, 0) << __func__ << "error opening pool " << task_info.dst_pool << ": "
	                  << cpp_strerror(r) << dendl;  
    goto out;
  }
#if 0  
  r = libradosstriper::RadosStriper::striper_create(*m_sfm_io_ctx, m_sfm_striper);
  if (0 != r)
  {
    ldout(m_cct, 0) << "error opening pool " << task_info.dst_pool << " with striper interface: "
                    << cpp_strerror(r) << dendl;  
    goto out;
  }
#endif  
  //r = m_sfm_striper->create(task_info.dst_file);
  r = m_sfm_io_ctx->create(task_info.dst_file, false);
  if (r < 0)
  {
    ldout(m_cct, 0) << "create sfm obj fail:" << cpp_strerror(r) << dendl;
    goto out;
  }
  
  task_info.stage = RGW_BGT_TASK_WAIT_MERGE;
  merge_stage = RGW_BGT_TASK_SFM_START;
  r = set_task_info(task_info);
  return r;
  
out:
#if 0
  delete m_sfm_striper;
  m_sfm_striper = NULL;
#endif
  delete m_sfm_io_ctx;
  m_sfm_io_ctx = NULL;  

  return r;
}

int RGWBgtWorker::set_task_info(RGWBgtTaskInfo& task_info)
{
  int r;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;
  
  ::encode(task_info, bl);
  values[RGW_BGT_TASK_META_KEY] = bl;
  r = m_instobj->m_io_ctx.omap_set(m_instobj->m_name, values);

  return r;
}

int RGWBgtWorker::get_task_info(RGWBgtTaskInfo& task_info)
{
  int r;
  std :: set < std :: string >keys;
  std :: map < std :: string,bufferlist > vals;
  keys.insert(RGW_BGT_TASK_META_KEY);
  r = m_instobj->m_io_ctx.omap_get_vals_by_keys(m_instobj->m_name, keys, &vals);
  if (0 == r)
  {
    if (0 == vals[RGW_BGT_TASK_META_KEY].length())
    {
      task_info.stage = RGW_BGT_TASK_FINISH;
      ldout(m_cct, 20) << "thers is no task info" << dendl;
    }
    else
    {
      bufferlist::iterator iter = vals[RGW_BGT_TASK_META_KEY].begin();
      ::decode(task_info, iter);
      ldout(m_cct, 20) << "load task info success:" << task_info.stage << dendl;
    }
  }
  else
  {
    ldout(m_cct, 0) << "load task info failed:" << cpp_strerror(r) << dendl;
  }

  return r;
}

void RGWBgtWorker::report_task_finished(RGWBgtTaskInfo& task_info)
{
  int r;

  r = notify_task_finished(task_info.task_id, m_cct->_conf->rgw_bgt_notify_timeout);
  if (0 == r)
  {
    task_info.stage = RGW_BGT_TASK_FINISH;
    set_task_info(task_info);
  }
}

int RGWBgtWorker::mk_sfm_index_from_change_log(bufferlist& bl, uint64_t rs_cnt, uint64_t& index_id)
{
  RGWChangeLogEntry* log_entry;
  char* buffer;
  RGWSfmIndex* index;
  RGWBucketInfo bucket_info;
  time_t mtime; 
  int r;

  librados::Rados *rad = m_store->get_rados_handle();
  
  buffer = bl.c_str();
  for (uint64_t j = 0; j < rs_cnt; j++)
  {
    log_entry = (RGWChangeLogEntry*)&buffer[j*RGW_CHANGE_LOG_ENTRY_SIZE];
    index = &sfm_index[index_id];

    index->bucket = log_entry->bucket;

    RGWObjectCtx obj_ctx(m_store);
    r = m_store->get_bucket_info(obj_ctx, index->bucket, bucket_info, &mtime);
    if (r < 0)
    {
      ldout(m_cct, 0) << "get bucket info failed:" << cpp_strerror(r) << dendl;
      return r;     
    }
    std :: map < string, librados::IoCtx>::iterator iter;

    iter = data_io_ctx.find(bucket_info.bucket.data_pool);
    if (iter == data_io_ctx.end())
    {
      r = rad->ioctx_create(bucket_info.bucket.data_pool.c_str(), data_io_ctx[bucket_info.bucket.data_pool]);
      if (r < 0)
      {
        ldout(m_cct, 0) << __func__ << "error opening pool " << bucket_info.bucket.data_pool << ": "
    	                  << cpp_strerror(r) << dendl;  
        return r;
      }      
    }
    iter = index_io_ctx.find(bucket_info.bucket.index_pool);
    if (iter == index_io_ctx.end())
    {
      r = rad->ioctx_create(bucket_info.bucket.index_pool.c_str(), index_io_ctx[bucket_info.bucket.index_pool]);
      if (r < 0)
      {
        ldout(m_cct, 0) << __func__ << "error opening pool " << bucket_info.bucket.index_pool << ": "
    	                  << cpp_strerror(r) << dendl;  
        return r;
      }      
    } 
    
    index->data_io_ctx = &data_io_ctx[bucket_info.bucket.data_pool];
    index->index_io_ctx = &index_io_ctx[bucket_info.bucket.index_pool];
    index->oid = bucket_info.bucket.bucket_id + "_" + log_entry->oid;
    index->bi_key = log_entry->oid;
    index->bi_oid = log_entry->bi_oid;
    index->off = 0;
    index->size = 0;

    index_id++;    
  }

  return 0;
}

int RGWBgtWorker::create_sfm_index(RGWBgtTaskInfo& task_info)
{
  if (merge_stage >= RGW_BGT_TASK_SFM_INDEX_CREATED)
    return 0;
    
  int r;
  
  uint32_t step = 4096;
  uint32_t cnt = task_info.count / step;
  uint32_t leave = task_info.count % step;

  uint64_t index_id = 0;

  uint64_t off = task_info.start*RGW_CHANGE_LOG_ENTRY_SIZE;
  uint64_t step_size = step*RGW_CHANGE_LOG_ENTRY_SIZE;

  sfm_index.erase(sfm_index.begin(), sfm_index.end());
  
  for (uint32_t i = 0; i < cnt; i++)
  {
    bufferlist bl;
    //r = m_instobj->m_striper.read(task_info.log_name, &bl, step_size, (off+i*step_size));
    r = m_instobj->m_io_ctx.read(task_info.log_name, bl, step_size, (off+i*step_size));
    if (r < 0)
    {
      ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
      return r;
    }
    r = mk_sfm_index_from_change_log(bl, step, index_id);
    if (r < 0)
    {
      return r;
    }
  }

  if (leave > 0)
  {
    bufferlist bl;
    //r = m_instobj->m_striper.read(task_info.log_name, &bl, RGW_CHANGE_LOG_ENTRY_SIZE*leave, off+cnt*step_size);
    r = m_instobj->m_io_ctx.read(task_info.log_name, bl, RGW_CHANGE_LOG_ENTRY_SIZE*leave, off+cnt*step_size);
    if (r < 0)
    {
      ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
      return r;
    }
    r = mk_sfm_index_from_change_log(bl, leave, index_id);
    if (r < 0)
    {
      return r;
    }
  }

  merge_stage = RGW_BGT_TASK_SFM_INDEX_CREATED;
  return 0;
}

int RGWBgtWorker::mk_sfm_index_from_buf(bufferlist& bl, uint64_t rs_cnt, uint64_t& index_id)
{
  RGWSfmIndexEntry* index_entry;
  char* buffer;
  RGWSfmIndex* index;
  RGWBucketInfo bucket_info;
  time_t mtime; 
  int r;

  librados::Rados *rad = m_store->get_rados_handle();
  
  buffer = bl.c_str();
  for (uint64_t j = 0; j < rs_cnt; j++)
  {
    index_entry = (RGWSfmIndexEntry*)&buffer[j*RGW_SFM_INDEX_ENTRY_SIZE];
    index = &sfm_index[index_id];

    RGWObjectCtx obj_ctx(m_store);
    r = m_store->get_bucket_info(obj_ctx, index_entry->bucket, bucket_info, &mtime);
    if (r < 0)
    {
      ldout(m_cct, 0) << "get bucket info failed:" << cpp_strerror(r) << dendl;
      return r;     
    }
    std :: map < string, librados::IoCtx>::iterator iter;

    iter = data_io_ctx.find(bucket_info.bucket.data_pool);
    if (iter == data_io_ctx.end())
    {
      r = rad->ioctx_create(bucket_info.bucket.data_pool.c_str(), data_io_ctx[bucket_info.bucket.data_pool]);
      if (r < 0)
      {
        ldout(m_cct, 0) << __func__ << "error opening pool " << bucket_info.bucket.data_pool << ": "
    	                  << cpp_strerror(r) << dendl;  
        return r;
      }      
    }
    iter = index_io_ctx.find(bucket_info.bucket.index_pool);
    if (iter == index_io_ctx.end())
    {
      r = rad->ioctx_create(bucket_info.bucket.index_pool.c_str(), index_io_ctx[bucket_info.bucket.index_pool]);
      if (r < 0)
      {
        ldout(m_cct, 0) << __func__ << "error opening pool " << bucket_info.bucket.index_pool << ": "
    	                  << cpp_strerror(r) << dendl;  
        return r;
      }      
    } 
    
    index->data_io_ctx = &data_io_ctx[bucket_info.bucket.data_pool];
    index->index_io_ctx = &index_io_ctx[bucket_info.bucket.index_pool];
    index->bi_key = index_entry->oid;
    index->oid = bucket_info.bucket.bucket_id + "_" + index->bi_key;
    index->bi_oid = index_entry->bi_oid;
    index->off = index_entry->off;
    index->size = index_entry->size;

    index_id++;    
  }

  return 0;
}

int RGWBgtWorker::load_sfm_index(RGWBgtTaskInfo & task_info)
{
  int r;
  
  uint32_t step = 4096;
  uint32_t cnt = task_info.count / step;
  uint32_t leave = task_info.count % step;

  uint64_t index_id = 0;

  uint64_t off = 0;
  uint64_t step_size = step*RGW_SFM_INDEX_ENTRY_SIZE;

  sfm_index.erase(sfm_index.begin(), sfm_index.end());
  
  for (uint64_t i = 0; i < cnt; i++)
  {
    bufferlist bl;
    r = m_instobj->m_io_ctx.read(m_instobj->m_name, bl, step_size, (off+i*step_size));
    if (r < 0)
    {
      ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
      return r;
    }
    r = mk_sfm_index_from_buf(bl, step, index_id);
    if (r < 0)
    {
      return r;
    }
  }

  if (leave > 0)
  {
    bufferlist bl;
    r = m_instobj->m_io_ctx.read(m_instobj->m_name, bl, RGW_SFM_INDEX_ENTRY_SIZE*leave, off+cnt*step_size);
    if (r < 0)
    {
      ldout(m_cct, 0) << "read failed:" << cpp_strerror(r) << dendl;
      return r;
    }
    r = mk_sfm_index_from_buf(bl, leave, index_id);
    if (r < 0)
    {
      return r;
    }
  }

  return 0;
}

void RGWBgtWorker::sfm_flush_data(RGWBgtTaskInfo& task_info)
{
  if (merge_stage >= RGW_BGT_TASK_SFM_DATA_FLUSHED)
    return;

  //int r = this->m_sfm_striper->write_full(task_info.dst_file, sfm_bl);
  //int r = this->m_sfm_striper->append(task_info.dst_file, sfm_bl, sfm_bl.length());
  int r = this->m_sfm_io_ctx->write_full(task_info.dst_file, sfm_bl);
  if (0 == r)
  {
    ldout(m_cct, 10) << "flush data success:" << sfm_bl.length() << dendl;
    merge_stage = RGW_BGT_TASK_SFM_DATA_FLUSHED;
    sfm_bl.clear();
  }
  else
  {
    ldout(m_cct, 0) << "flush data is failed:" << cpp_strerror(r) << dendl;
  }
}

void RGWBgtWorker::sfm_flush_index(RGWBgtTaskInfo& task_info)
{
  if (merge_stage < RGW_BGT_TASK_SFM_DATA_FLUSHED ||
      merge_stage >= RGW_BGT_TASK_SFM_INDEX_FLUSHED)
    return;

  int r;  
  bufferlist bl_index;
  std :: map < uint64_t, RGWSfmIndex>::iterator iter = sfm_index.begin();
  RGWSfmIndexEntry index_entry;
  while (iter != sfm_index.end())
  {
    strcpy(index_entry.bucket, iter->second.bucket.c_str());
    strcpy(index_entry.oid, iter->second.bi_key.c_str());
    index_entry.off = iter->second.off;
    index_entry.size = iter->second.size;
    
    bl_index.append((char*)&index_entry, sizeof(RGWSfmIndexEntry));
    iter++;
  }

  librados::ObjectWriteOperation writeOp;
  bufferlist bl;
  std :: map < std :: string,bufferlist > values;

  task_info.stage = RGW_BGT_TASK_WAIT_UPDATE_INDEX;
  ::encode(task_info, bl);
  values[RGW_BGT_TASK_META_KEY] = bl;

  writeOp.write_full(bl_index);
  writeOp.omap_set(values);

  r = m_instobj->m_io_ctx.operate(m_instobj->m_name, &writeOp);
  if (0 == r)
  {
    merge_stage = RGW_BGT_TASK_SFM_INDEX_FLUSHED;
  }
  else
  {
    ldout(m_cct, 0) << "flush index is failed:" << cpp_strerror(r) << dendl;
  }
}

void RGWBgtWorker::set_sfm_result(RGWBgtTaskInfo& task_info)
{
  sfm_flush_data(task_info);
  sfm_flush_index(task_info);
}

void RGWBgtWorker::process_merge_result(RGWBgtTaskInfo& task_info)
{
  std :: map < uint64_t, RGWSfmIndex>::iterator idxi;
  list<bufferlist>::iterator bli;
  RGWSfmIndex* idx;
  
  sfm_bl.clear();
  for (idxi = sfm_index.begin(); idxi != sfm_index.end(); ++idxi)
  {
    idx = &idxi->second;
    idx->off = sfm_bl.length();
    idx->size = 0;
    for (bli = idx->lbl.begin(); bli != idx->lbl.end(); ++bli)
    {
      bufferlist& bl = *bli;
      ldout(m_cct, 10) << "io len = " << bl.length() << dendl;
      idx->size += bl.length(); 
      sfm_bl.claim_append(bl);
    }
  }

  ldout(m_cct, 10) << "Total merged size is " << sfm_bl.length() << dendl;
}

void RGWBgtWorker::wait_merge(RGWBgtTaskInfo& task_info)
{
  ldout(m_cct, 10) << "merge state:" << merge_stage << dendl;
  if (0 == create_sfm_index(task_info))
  {
    if (merge_stage < RGW_BGT_TASK_SFM_MERGED)
    {
      lock.Lock();
      //send merge commond
      /*Begin modified by guokexin*/
      ldout(m_cct, 10) << " Notify ArchiveTask Process Merge" <<dendl;
      m_archive_task.dispatch_task(1);
      /*End modified*/
      cond.Wait(lock);
      lock.Unlock();
      
      process_merge_result(task_info);
      merge_stage = RGW_BGT_TASK_SFM_MERGED;  
    }
    set_sfm_result(task_info);
  }
}

void RGWBgtWorker::update_index(RGWBgtTaskInfo& task_info)
{
  std :: map < uint64_t, RGWSfmIndex>::iterator idxi;
  RGWSfmIndex* idx;
  int r;
  for (idxi = sfm_index.begin(); idxi != sfm_index.end(); ++idxi)
  {
    std :: set < std :: string > keys;
    std :: map < std :: string,bufferlist > vals;
    
    idx = &idxi->second;
    keys.insert(idx->bi_key);
    r = idx->index_io_ctx->omap_get_vals_by_keys(idx->bi_oid, keys, &vals);
    if (r < 0 || 0 == vals[idx->bi_key].length())
    {
      ldout(m_cct, 0) << "get obj index meta failed:" 
                      << "bucket=" << idx->bucket << "," 
                      << idx->bi_oid << "/" << idx->bi_key << dendl;
      goto err;                
    }
    else
    {
      rgw_bucket_dir_entry bi_entry;
      
      bufferlist::iterator iter = vals[idx->bi_key].begin();
      try
      {
        ::decode(bi_entry, iter);
      }catch (buffer::error& e)
      {
        ldout(m_cct, 0) << "decode bi_entry failed" << dendl;
        goto err;
      }
      
      bi_entry.meta.is_merged = true;
      bi_entry.meta.data_pool = task_info.dst_pool;
      bi_entry.meta.data_oid = task_info.dst_file;
      bi_entry.meta.data_offset = idx->off;
      bi_entry.meta.data_size = idx->size;
      
      bufferlist bl;
      std :: map < std :: string,bufferlist > vals;
      ::encode(bi_entry, bl);
      vals[idx->bi_key].swap(bl);
      r = idx->index_io_ctx->omap_set(idx->bi_oid, vals);
      if (r < 0)
      {
        ldout(m_cct, 0) << "update object index failed:" << cpp_strerror(r) << dendl;
        goto err;
      }
    }
  }

  ldout(m_cct, 10) << "update all object index success" << dendl;
  
  task_info.stage = RGW_BGT_TASK_WAIT_DEL_DATA;
  set_task_info(task_info);
  return;
  
err:
  return;
}

void RGWBgtWorker::delete_data(RGWBgtTaskInfo& task_info)
{
  ldout(m_cct, 10) << "Delete data, Wait to do!" << dendl;

  /*Begin added by guokexin*/
  lock.Lock();
  //send delete commond
  m_archive_task.dispatch_task(2);
  cond.Wait(lock);
  lock.Unlock();
  /*End added*/

  task_info.stage = RGW_BGT_TASK_WAIT_REPORT_FINISH;
  set_task_info(task_info);  
}

void RGWBgtWorker::check_task()
{
  RGWBgtTaskInfo task_info;
  int r;

  r = get_task_info(task_info);
  if (0 != r)
  {
    return;
  }

  switch (task_info.stage)
  {
    case RGW_BGT_TASK_WAIT_CREATE_SFMOBJ:
      {
        create_sfm_obj(task_info);
      }
      break;

    case RGW_BGT_TASK_WAIT_MERGE:
      {
        ldout(m_cct, 10) << "RGW_BGT_TASK_WAIT_MERGE" << dendl;
        wait_merge(task_info);
      }
      break;

    case RGW_BGT_TASK_WAIT_UPDATE_INDEX:
      {
        update_index(task_info);
      }
      break;

    case RGW_BGT_TASK_WAIT_DEL_DATA:
      {
        delete_data(task_info);
      }
      break;

    case RGW_BGT_TASK_WAIT_REPORT_FINISH:
      {
        report_task_finished(task_info);
      }
      break;

    case RGW_BGT_TASK_FINISH:
      {
        this->sfm_index.clear();
        this->sfm_bl.clear();
        this->merge_stage = RGW_BGT_TASK_SFM_START;
        ldout(m_cct, 10) << "Task finished, wait next task!" << dendl;
      }
      break;

    default:
      {
        assert(0);
      }
      break;
  }
}

void RGWBgtWorker::load_task()
{
  int r;
  RGWBgtTaskInfo task_info;

  r = get_task_info(task_info);
  if (0 != r)
  {
    ldout(m_cct, 0) << "get task info failed" << dendl;
    assert(0);
  } 
  else
  {
    ldout(m_cct, 0) << "current task stage is: " << task_info.stage << dendl;
  }

  if (task_info.stage > RGW_BGT_TASK_WAIT_CREATE_SFMOBJ && 
      task_info.stage < RGW_BGT_TASK_WAIT_REPORT_FINISH)
  {
#if 0  
    m_sfm_striper = new libradosstriper::RadosStriper();
    assert(NULL != m_sfm_striper);
#endif
    m_sfm_io_ctx = new librados::IoCtx();
    assert(NULL != m_sfm_io_ctx);

    bufferlist bl;
    librados::Rados *rad = m_store->get_rados_handle();
    int r = rad->ioctx_create(task_info.dst_pool.c_str(), *m_sfm_io_ctx);
    if (r < 0)
    {
      ldout(m_cct, 0) << __func__ << "error opening pool " << task_info.dst_pool << ": "
  	                  << cpp_strerror(r) << dendl;  
      goto out;
    }
#if 0    
    r = libradosstriper::RadosStriper::striper_create(*m_sfm_io_ctx, m_sfm_striper);
    if (0 != r)
    {
      ldout(m_cct, 0) << "error opening pool " << task_info.dst_pool << " with striper interface: "
                      << cpp_strerror(r) << dendl;  
      goto out;
    }
#endif    
    goto done;
    
out:
    assert(0);
    return;
  }
  
done:  
  if (RGW_BGT_TASK_WAIT_UPDATE_INDEX == task_info.stage || 
      RGW_BGT_TASK_WAIT_DEL_DATA == task_info.stage)
  {
    r = load_sfm_index(task_info);
    if (0 != r)
    {
      ldout(m_cct, 0) << "load sfm index failed" << dendl;
      assert(0);
    }
  }
}

void* RGWBgtWorker::entry()
{
  while (!stopping)
  {
    uint32_t sec = m_cct->_conf->rgw_bgt_tick_interval/1000;
    uint32_t nsec = (m_cct->_conf->rgw_bgt_tick_interval % 1000)*1000*1000; 

    check_task();

    lock.Lock();
    cond.WaitInterval(NULL, lock, utime_t(sec, nsec));
    lock.Unlock();
  }

  return NULL;

}
void RGWBgtWorker::start()
{
  {
    Mutex::Locker l(lock);
    stopping = false;
  }
  create();

}
void RGWBgtWorker::stop()
{
  {
    Mutex::Locker l(lock);
    stopping = true;
    cond.Signal();
  }

  join();
}

