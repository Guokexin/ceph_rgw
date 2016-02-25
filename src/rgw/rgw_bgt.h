// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGWBGT_H
#define CEPH_RGWBGT_H

#include "include/rados/librados.hpp"
#if 0
#include "include/radosstriper/libradosstriper.hpp"
#endif
#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/statelog/cls_statelog_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_rest_conn.h"
#include <boost/variant/variant.hpp>
#include "include/stringify.h"
#include "rgw_archive_task.h"


class RGWInstanceObj;
class RGWInstObjWatchCtx;
class RGWInstanceObjWatcher;
class RGWBgtScheduler;
class RGWBgtWorker;
class ArchiveTask;


#define RGW_BGT_SCHEDULER_INST	"xsky.bgt.scheduler.inst"
#define RGW_BGT_WORKER_INST_PREFIX "xsky.bgt.worker.inst."
#define RGW_BGT_BATCH_TASK_META_KEY	"xsky.bgt_meta_batch_task"
#define RGW_BGT_TASK_META_KEY       "xsky.bgt_meta_task"
#define RGW_BGT_ACTIVE_CHANGE_LOG_KEY "xsky.bgt_active_change_log"
#define RGW_BGT_TASK_PREFIX "xsky.bgt_task_"
#define RGW_BGT_CHANGELOG_PREFIX "xsky.bgt_log_"
#define RGW_BGT_LOG_TRANS_META_KEY "xsky.bgt_meta_log_trans"
#define RGW_BGT_MERGEFILE_PREFIX "xsky.bgt_sfm_"

#define RGW_ROLE_IO_PROCESS    0x1 
#define RGW_ROLE_BGT_SCHEDULER 0x2
#define RGW_ROLE_BGT_WORKER    0x4

#define RGW_INSTOBJ_NOTIFY_TIMEOUT 30000000

enum RGWInstObjWatchState {
	RGW_INSTOBJWATCH_STATE_UNREGISTERED,
	RGW_INSTOBJWATCH_STATE_REGISTERED,
	RGW_INSTOBJWATCH_STATE_ERROR
};

enum RGW_BGT_WORKER_STATE
{
	RGW_BGT_WORKER_INACTIVE					= 0,
	RGW_BGT_WORKER_ACTIVE						= 1	
};
std::ostream &operator<<(std::ostream &out, const RGW_BGT_WORKER_STATE &state);


enum RGWInstObjNotifyOp
{
	RGW_NOTIFY_OP_REGISTER							= 0,
	RGW_NOTIFY_OP_REGISTER_RSP					= 1,	
	RGW_NOTIFY_OP_ACTIVE_CHANGELOG 			= 2,
	RGW_NOTIFY_OP_ACTIVE_CHANGELOG_RSP 	= 3,
	RGW_NOTIFY_OP_DISPATCH_TASK					= 4,
	RGW_NOTIFY_OP_DISPATCH_TASK_RSP 		= 5,
	RGW_NOTIFY_OP_TASK_FINISH						= 6,
	RGW_NOTIFY_OP_TASK_FINISH_RSP				= 7
};
std::ostream &operator<<(std::ostream &out, const RGWInstObjNotifyOp &op);

std::string rgw_unique_lock_name(const std::string &name, void *address);

class RGWInstObjEncodePayloadVisitor : public boost::static_visitor<void> {
public:
  RGWInstObjEncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    ::encode(static_cast<uint32_t>(Payload::NOTIFY_OP), m_bl);
    payload.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class RGWInstObjDecodePayloadVisitor : public boost::static_visitor<void> {
public:
  RGWInstObjDecodePayloadVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {}

  template <typename Payload>
  inline void operator()(Payload &payload) const {
    payload.decode(m_version, m_iter);
  }

private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

class RGWInstObjDumpPayloadVisitor : public boost::static_visitor<void> {
public:
  RGWInstObjDumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    RGWInstObjNotifyOp notify_op = Payload::NOTIFY_OP;
    m_formatter->dump_string("notify_op", stringify(notify_op));
    payload.dump(m_formatter);
  }

private:
  ceph::Formatter *m_formatter;
};

struct WorkerRegisterPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_REGISTER;
	string worker_name;
	uint64_t task_id;

	WorkerRegisterPayload(){};
	WorkerRegisterPayload(const string &name, uint64_t _task_id) : worker_name(name), task_id(_task_id){};
	
	void encode(bufferlist& bl) const
	{
		::encode(worker_name, bl);
		::encode(task_id, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(worker_name, iter);
		::decode(task_id, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_string("worker_name", worker_name);
		f->dump_unsigned("task_id", task_id);
	}
};

struct WorkerRegisterRspPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_REGISTER_RSP;
	string active_change_log;

	WorkerRegisterRspPayload(){};
	WorkerRegisterRspPayload(const string &log_name) : active_change_log(log_name){};
	
	void encode(bufferlist& bl) const
	{
		::encode(active_change_log, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(active_change_log, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_string("active_change_log", active_change_log);
	}
};

struct ActiveChangeLogPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_ACTIVE_CHANGELOG;
	string log_name;

	ActiveChangeLogPayload(){};
	ActiveChangeLogPayload(const string &name) : log_name(name){};
	
	void encode(bufferlist& bl) const
	{
		::encode(log_name, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(log_name, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_string("log_name", log_name);
	}	
};

struct ActiveChangeLogRspPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_ACTIVE_CHANGELOG_RSP;
	string worker_name;

	ActiveChangeLogRspPayload(){};
	ActiveChangeLogRspPayload(const string &name) : worker_name(name){};
	
	void encode(bufferlist& bl) const
	{
		::encode(worker_name, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(worker_name, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_string("worker_name", worker_name);
	}	
};

struct DispatchTaskPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_DISPATCH_TASK;
	uint64_t task_id;
	string log_name;
	uint64_t start;
	uint32_t count;	

	DispatchTaskPayload(){};
	DispatchTaskPayload(uint64_t _task_id, const string &_log_name, uint64_t _start, uint32_t _count) : 
		                        task_id(_task_id),
														log_name(_log_name),
														start(_start),
														count(_count)
{
};
	
	void encode(bufferlist& bl) const
	{
		::encode(task_id, bl);
		::encode(log_name, bl);
		::encode(start, bl);
		::encode(count, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(task_id, iter);
		::decode(log_name, iter);
		::decode(start, iter);
		::decode(count, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_unsigned("task_id", task_id);
		f->dump_string("log_name", log_name);
		f->dump_unsigned("start", start);
		f->dump_unsigned("count", count);
	}	
};

struct DispatchTaskRspPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_DISPATCH_TASK_RSP;
	uint64_t task_id;
	string worker_name;

	DispatchTaskRspPayload(){};
	DispatchTaskRspPayload(uint64_t _task_id, const string &_worker_name) : 
		                            task_id(_task_id),
														    worker_name(_worker_name)
{
};
	
	void encode(bufferlist& bl) const
	{
		::encode(task_id, bl);
		::encode(worker_name, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(task_id, iter);
		::decode(worker_name, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_unsigned("task_id", task_id);
		f->dump_string("worker_name", worker_name);
	}	
};

struct TaskFinishedPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_TASK_FINISH;
	uint64_t task_id;
	time_t finish_time;
        string worker_name;

	TaskFinishedPayload(){};
	TaskFinishedPayload(uint64_t _task_id, time_t _finish_time,string _work_name) : task_id(_task_id), finish_time(_finish_time),worker_name(_work_name){};
	
	void encode(bufferlist& bl) const
	{
		::encode(task_id, bl);
		::encode(finish_time, bl);
                ::encode(worker_name,bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(task_id, iter);
		::decode(finish_time, iter);
                ::decode(worker_name,iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_unsigned("task_id", task_id);
		f->dump_unsigned("finish_time", finish_time);
                f->dump_string("worker_name",worker_name);
	}		
};

struct TaskFinishedRspPayload
{
	static const RGWInstObjNotifyOp NOTIFY_OP = RGW_NOTIFY_OP_TASK_FINISH_RSP;
	uint64_t task_id;

	TaskFinishedRspPayload(){};
	TaskFinishedRspPayload(uint64_t _task_id) : task_id(_task_id){};
	
	void encode(bufferlist& bl) const
	{
		::encode(task_id, bl);
	}
	void decode(__u8 version, bufferlist::iterator &iter)
	{
		::decode(task_id, iter);
	}
	void dump(Formatter *f) const
	{
		f->dump_unsigned("task_id", task_id);
	}		
};

struct RGWInstObjUnknownPayload 
{
  static const RGWInstObjNotifyOp NOTIFY_OP = static_cast<RGWInstObjNotifyOp>(-1);

  void encode(bufferlist &bl) const
  {
  	assert(false);
  }
  void decode(__u8 version, bufferlist::iterator &iter)
  {
  }
  void dump(Formatter *f) const
  {
  }
};

typedef boost::variant<WorkerRegisterPayload,
                       ActiveChangeLogPayload,
                       DispatchTaskPayload,
                       TaskFinishedPayload,
                       RGWInstObjUnknownPayload> RGWInstObjPayload;


struct RGWInstObjNotifyMsg
{
	RGWInstObjPayload payload;

  RGWInstObjNotifyMsg() : payload(RGWInstObjUnknownPayload()) {}
  RGWInstObjNotifyMsg(const RGWInstObjPayload &payload_) : payload(payload_) {}	

  void encode(bufferlist& bl) const
  {
  	ENCODE_START(1, 1, bl);
		boost::apply_visitor(RGWInstObjEncodePayloadVisitor(bl), payload);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	DECODE_START(1, iter);

		uint32_t notify_op;
		::decode(notify_op, iter);

		switch (notify_op)
		{
			case RGW_NOTIFY_OP_REGISTER:
				payload = WorkerRegisterPayload();
				break;

			case RGW_NOTIFY_OP_ACTIVE_CHANGELOG:
				payload = ActiveChangeLogPayload();
				break;

			case RGW_NOTIFY_OP_DISPATCH_TASK:
				payload = DispatchTaskPayload();
				break;

			case RGW_NOTIFY_OP_TASK_FINISH:
                                /*guokexin...*/                     
				payload = TaskFinishedPayload();
				break;

			default:
				payload = RGWInstObjUnknownPayload();
				break;
		}

		apply_visitor(RGWInstObjDecodePayloadVisitor(struct_v, iter), payload);
		DECODE_FINISH(iter);
  }
	
  void dump(Formatter *f) const
  {
  	apply_visitor(RGWInstObjDumpPayloadVisitor(f), payload);
  }
};
WRITE_CLASS_ENCODER(RGWInstObjNotifyMsg);

std::ostream &operator<<(std::ostream &out, const RGWInstObjNotifyOp &op);


typedef boost::variant<WorkerRegisterRspPayload,
                       ActiveChangeLogRspPayload,
                       DispatchTaskRspPayload,
                       TaskFinishedRspPayload,
                       RGWInstObjUnknownPayload> RGWInstObjRspPayload;


struct RGWInstObjNotifyRspMsg
{
	int32_t result;
	RGWInstObjRspPayload payload;

  RGWInstObjNotifyRspMsg() : result(-1), payload(RGWInstObjUnknownPayload()) {}
  RGWInstObjNotifyRspMsg(const int32_t result_, const RGWInstObjRspPayload &payload_) : 
                                 result(result_), 
																 payload(payload_) {}	

  void encode(bufferlist& bl) const
  {
  	ENCODE_START(1, 1, bl);
		::encode(result, bl);
		boost::apply_visitor(RGWInstObjEncodePayloadVisitor(bl), payload);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	DECODE_START(1, iter);
		uint32_t notify_op;

		::decode(result, iter);
		::decode(notify_op, iter);

		switch (notify_op)
		{
			case RGW_NOTIFY_OP_REGISTER_RSP:
				payload = WorkerRegisterRspPayload();
				break;

			case RGW_NOTIFY_OP_ACTIVE_CHANGELOG_RSP:
				payload = ActiveChangeLogRspPayload();
				break;

			case RGW_NOTIFY_OP_DISPATCH_TASK_RSP:
				payload = DispatchTaskRspPayload();
				break;

			case RGW_NOTIFY_OP_TASK_FINISH_RSP:
				payload = TaskFinishedRspPayload();
				break;

			default:
				payload = RGWInstObjUnknownPayload();
				break;
		}
		
		apply_visitor(RGWInstObjDecodePayloadVisitor(struct_v, iter), payload);
		DECODE_FINISH(iter);
  }
	
  void dump(Formatter *f) const
  {
  	apply_visitor(RGWInstObjDumpPayloadVisitor(f), payload);
  }
};
WRITE_CLASS_ENCODER(RGWInstObjNotifyRspMsg);

struct RGWBgtTaskEntry
{
 	uint64_t start;
	uint32_t count;
	time_t dispatch_time;
	time_t finish_time;
	bool is_finished;

	RGWBgtTaskEntry(): start(0), count(0), 
		                       dispatch_time(utime_t()), finish_time(utime_t()), is_finished(false)
	{
	}
  void encode(bufferlist& bl) const
  {
  	ENCODE_START(1, 1, bl);
 		::encode(start, bl);
		::encode(count, bl);
		::encode(dispatch_time, bl);
		::encode(finish_time, bl);
		::encode(is_finished, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	DECODE_START(1, iter);
 		::decode(start, iter);
		::decode(count, iter);
		::decode(dispatch_time, iter);
		::decode(finish_time, iter);
		::decode(is_finished, iter);
		DECODE_FINISH(iter);
  }
	
  void dump(Formatter *f) const
  {
 		f->dump_unsigned("start", start);
		f->dump_unsigned("count", count);
		f->dump_unsigned("dispatch_time", dispatch_time);
		f->dump_unsigned("finish_time", finish_time);
		f->dump_bool("is_finished", is_finished);
	}
};
WRITE_CLASS_ENCODER(RGWBgtTaskEntry);

struct RGWBgtChangeLogInfo
{
	uint16_t state;

	RGWBgtChangeLogInfo() : state(0)
	{
	}
	
  void encode(bufferlist& bl) const
  {
  	ENCODE_START(1, 1, bl);
		::encode(state, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	DECODE_START(1, iter);
		::decode(state, iter);
		DECODE_FINISH(iter);
  }
	
  void dump(Formatter *f) const
  {
  	f->dump_unsigned("state", state);
	}
};
WRITE_CLASS_ENCODER(RGWBgtChangeLogInfo);

struct RGWBgtWorkerInfo
{
	RGW_BGT_WORKER_STATE state;
	uint16_t idle;

	RGWBgtWorkerInfo() : state(RGW_BGT_WORKER_INACTIVE)
	{
	}
	
  void encode(bufferlist& bl) const
  {
  	uint8_t _state = state;
  	ENCODE_START(1, 1, bl);
		::encode(_state, bl);
		::encode(idle, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	uint8_t _state;
  	DECODE_START(1, iter);
		::decode(_state, iter);
		::decode(idle, iter);
		DECODE_FINISH(iter);
		state = static_cast<RGW_BGT_WORKER_STATE>(_state);
  }
	
  void dump(Formatter *f) const
  {
  	f->dump_unsigned("state", state);
		f->dump_unsigned("idle", idle);
	}
};
WRITE_CLASS_ENCODER(RGWBgtWorkerInfo);

enum RGWBgtTaskState
{
	RGW_BGT_TASK_WAIT_CREATE_SFMOBJ = 0,//wait to create big file obj
	RGW_BGT_TASK_WAIT_MERGE = 1, 				//wait to merge small file and write the big file
	RGW_BGT_TASK_WAIT_UPDATE_INDEX = 2, //wait to update meta of small file
	RGW_BGT_TASK_WAIT_DEL_DATA = 3,     //wait to delete data of small file  
	RGW_BGT_TASK_WAIT_REPORT_FINISH = 4,//wait to report to scheduler
	RGW_BGT_TASK_FINISH = 5							//task is finished
};
std::ostream &operator<<(std::ostream &out, const RGWBgtTaskState &state);

enum RGWBgtTaskSFMergeState
{
	RGW_BGT_TASK_SFM_START = 0,
	RGW_BGT_TASK_SFM_INDEX_CREATED = 1,
	RGW_BGT_TASK_SFM_MERGED = 2,
	RGW_BGT_TASK_SFM_DATA_FLUSHED = 3,
	RGW_BGT_TASK_SFM_INDEX_FLUSHED = 4
};
std::ostream &operator<<(std::ostream &out, const RGWBgtTaskSFMergeState &state);

struct RGWBgtTaskInfo
{
	uint64_t task_id;
	string log_name;
	uint64_t start;
	uint32_t count;
	RGWBgtTaskState stage;
	string dst_pool;
	string dst_file;

  void encode(bufferlist& bl) const
  {
  	uint8_t _stage = stage;
  	ENCODE_START(1, 1, bl);
		::encode(task_id, bl);
		::encode(log_name, bl);
		::encode(start, bl);
		::encode(count, bl);
		::encode(_stage, bl);
		::encode(dst_pool, bl);
		::encode(dst_file, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	uint8_t _stage;
  	DECODE_START(1, iter);
		::decode(task_id, iter);
		::decode(log_name, iter);
		::decode(start, iter);
		::decode(count, iter);
		::decode(_stage, iter);
		::decode(dst_pool, iter);
		::decode(dst_file, iter);
		DECODE_FINISH(iter);
		stage = static_cast<RGWBgtTaskState>(_stage);
  }
	
  void dump(Formatter *f) const
  {
  	f->dump_unsigned("task_id", task_id);
		f->dump_string("log_name", log_name);
		f->dump_unsigned("start", start);
		f->dump_unsigned("count", count);
		f->dump_unsigned("stage", stage);
		f->dump_string("dst_pool", dst_pool);
		f->dump_string("dst_file", dst_file);
	}	
};
WRITE_CLASS_ENCODER(RGWBgtTaskInfo);

enum RGWBgtBatchTaskState
{
	RGW_BGT_BATCH_TASK_WAIT_CREATE = 0, 						//wait to create task
	RGW_BGT_BATCH_TASK_WAIT_DISPATCH = 1, 					//wait to dispatch task
	RGW_BGT_BATCH_TASK_WAIT_RM_LOG = 2,							//wait to clear log file
	RGW_BGT_BATCH_TASK_WAIT_RM_LOG_ENTRY = 3,				//wait to remove log entry in change logs
	RGW_BGT_BATCH_TASK_WAIT_CLEAR_TASK_ENTRY = 4,		//wait to clear task entry
	RGW_BGT_BATCH_TASK_FINISH = 5     				  		//batch task is finished
};
std::ostream &operator<<(std::ostream &out, const RGWBgtBatchTaskState &state);

enum RGWBgtLogTransState
{
	RGW_BGT_LOG_TRANS_START = 0,
	RGW_BGT_LOG_TRANS_WAIT_CREATE_LOG_OBJ = 1,
	RGW_BGT_LOG_TRANS_WAIT_ADD_TO_INACTIVE = 2,
	RGW_BGT_LOG_TRANS_WAIT_SET_ACTIVE = 3,
	RGW_BGT_LOG_TRANS_WAIT_NOTIFY_WORKER = 4,
	RGW_BGT_LOG_TRANS_FINISH = 5
};
std::ostream &operator<<(std::ostream &out, const RGWBgtLogTransState &state);

struct RGWBgtLogTransInfo
{
	RGWBgtLogTransState stage;
	string active_change_log;
	string pending_active_change_log;

	RGWBgtLogTransInfo() : stage(RGW_BGT_LOG_TRANS_FINISH), 
		                             active_change_log(""),
		                             pending_active_change_log("")
	{
	}
  void encode(bufferlist& bl) const
  {
  	uint8_t _stage = stage;
  	ENCODE_START(1, 1, bl);
		::encode(_stage, bl);
		::encode(active_change_log, bl);
		::encode(pending_active_change_log, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	uint8_t _stage;
  	DECODE_START(1, iter);
		::decode(_stage, iter);
		::decode(active_change_log, iter);
		::decode(pending_active_change_log, iter);
		DECODE_FINISH(iter);
		stage = static_cast<RGWBgtLogTransState>(_stage);
  }
	
  void dump(Formatter *f) const
  {
		f->dump_unsigned("stage", stage);
		f->dump_string("active_change_log", active_change_log);
		f->dump_string("pending_active_change_log", pending_active_change_log);
	}		
};
WRITE_CLASS_ENCODER(RGWBgtLogTransInfo);

struct RGWChangeLogEntry
{
	char bucket[64];
	char oid[512];
	char bi_oid[64];
	uint64_t size;
};
#define RGW_CHANGE_LOG_ENTRY_SIZE sizeof(RGWChangeLogEntry)

struct RGWSfmIndex
{
	librados::IoCtx *data_io_ctx;
	librados::IoCtx *index_io_ctx;
	string bucket;
	string oid;
	string bi_key;
	string bi_oid;
	uint64_t off;
	uint64_t size;
	list<bufferlist> lbl;
};

struct RGWSfmIndexEntry
{
	char bucket[64];
	char oid[512];
	char bi_oid[64];
	uint64_t off;
	uint64_t size;
};
#define RGW_SFM_INDEX_ENTRY_SIZE  sizeof(RGWSfmIndexEntry)

struct RGWBgtBatchTaskInfo
{
	string log_name;
	RGWBgtBatchTaskState stage;
	uint64_t task_cnt;
	uint64_t next_task;

  void encode(bufferlist& bl) const
  {
  	uint8_t _stage = stage;
  	ENCODE_START(1, 1, bl);
		::encode(log_name, bl);
		::encode(_stage, bl);
		::encode(task_cnt, bl);
		::encode(next_task, bl);
		ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& iter)
  {
  	uint8_t _stage;
  	DECODE_START(1, iter);
		::decode(log_name, iter);
		::decode(_stage, iter);
		::decode(task_cnt, iter);
		::decode(next_task, iter);
		DECODE_FINISH(iter);
		stage = static_cast<RGWBgtBatchTaskState>(_stage);
  }
	
  void dump(Formatter *f) const
  {
		f->dump_string("log_name", log_name);
		f->dump_unsigned("stage", stage);
		f->dump_unsigned("task_cnt", task_cnt);
		f->dump_unsigned("next_task", next_task);
	}		
};
WRITE_CLASS_ENCODER(RGWBgtBatchTaskInfo);

class RGWInstObjWatchCtx : public librados::WatchCtx2 
{
public:

	RGWInstObjWatchCtx(RGWInstanceObjWatcher *parent, CephContext* cct) : 
		                        m_instobj_watcher(parent),
														m_cct(cct)	
														{}
  ~RGWInstObjWatchCtx(){}
	virtual void handle_notify(uint64_t notify_id,
																uint64_t handle,
														    uint64_t notifier_id,
																bufferlist& bl);
	virtual void handle_error(uint64_t handle, int err);
private:
	RGWInstanceObjWatcher *m_instobj_watcher;
	CephContext* m_cct;
};	

class RGWInstanceObj
{
	friend class RGWInstanceObjWatcher;
	friend class RGWInstObjWatchCtx;
	friend class RGWBgtWorker;
	friend class RGWBgtScheduler;
public:
  RGWInstanceObj(const std::string &pool_name, const std::string &instobj_name, RGWRados* store, CephContext* cct);
  ~RGWInstanceObj();	

	int init();
  int register_watch(uint64_t* handle, RGWInstObjWatchCtx* ctx);
  int unregister_watch(uint64_t handle);

private:
  CephContext *m_cct;
	RGWRados *m_store;
	string m_pool;
	string m_name;
	librados::IoCtx m_io_ctx;
#if 0	
	libradosstriper::RadosStriper m_striper;
#endif
	RGWInstanceObjWatcher *m_instobj_watcher;	
};

struct C_RGWInstObjNotifyAck : public Context {
	RGWInstanceObjWatcher *inst_obj_watcher;
	uint64_t notify_id;
	uint64_t handle;
	bufferlist out;

	C_RGWInstObjNotifyAck(RGWInstanceObjWatcher *inst_obj_watcher, uint64_t notify_id, uint64_t handle);
	virtual void finish(int r);
};

struct C_RGWInstObjResponseMessage : public Context {
	C_RGWInstObjNotifyAck *notify_ack;

	C_RGWInstObjResponseMessage(C_RGWInstObjNotifyAck *notify_ack) : notify_ack(notify_ack) {
	}
	virtual void finish(int r);
};
#if 0
struct C_ProcessPayload : public Context {
	RGWInstanceObjWatcher *inst_obj_watcher;
	uint64_t notify_id;
	uint64_t handle;
	RGWInstObjPayload payload;

	C_ProcessPayload(RGWInstanceObjWatcher *inst_obj_watcher_, uint64_t notify_id_,
									      uint64_t handle_, const RGWInstObjPayload &payload_) : 
									      inst_obj_watcher(inst_obj_watcher_), 
												notify_id(notify_id_), 
												handle(handle_),
												payload(payload_) 
	{
	}

	virtual void finish(int r) override 
	{
		inst_obj_watcher->process_payload(notify_id, handle, payload, r);
	}
};
#endif

class RGWInstanceObjWatcher
{
	friend class RGWRados;
	friend class RGWInstObjWatchCtx;
	friend class C_RGWInstObjNotifyAck;
public:
  RGWInstanceObjWatcher(RGWInstanceObj* instobj, int role, RGWRados* store, CephContext* cct);
  virtual ~RGWInstanceObjWatcher();

  int register_watch();
  int unregister_watch();

	void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl); 
	void handle_error(uint64_t handle, int err);

	void process_payload(uint64_t notify_id, uint64_t handle, const RGWInstObjPayload &payload, int r);
	void acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &ack_bl);
	int get_notify_rsp(bufferlist& ack_bl, RGWInstObjNotifyRspMsg& notify_rsp);

	virtual int notify_register(uint64_t timeout_ms) {return 0;}
	virtual int notify_active_changelog(const string& worker_name, const string& log_name, uint64_t timeout_ms){return 0;}
	virtual int notify_dispatch_task(const string& worker_name, uint64_t task_id, const string& log_name, uint64_t start, uint32_t count, uint64_t timeout_ms){return 0;}
	virtual int notify_task_finished(uint64_t task_id, uint64_t timeout_ms){return 0;}

	virtual bool handle_payload(const WorkerRegisterPayload& payload,	C_RGWInstObjNotifyAck *ctx){return true;}
	virtual bool handle_payload(const ActiveChangeLogPayload& payload,	C_RGWInstObjNotifyAck *ctx){return true;}
	virtual bool handle_payload(const DispatchTaskPayload& payload,	C_RGWInstObjNotifyAck *ctx){return true;}
	virtual bool handle_payload(const TaskFinishedPayload& payload,	C_RGWInstObjNotifyAck *ctx){return true;}
	virtual bool handle_payload(const RGWInstObjUnknownPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}

protected:
	CephContext* m_cct;
	RGWRados *m_store;
  RGWInstanceObj *m_instobj;
  RGWInstObjWatchCtx m_watch_ctx;
  uint64_t m_watch_handle;
	int m_watch_state;
	int m_state;
	int m_role;
};

struct RGWInstObjHandlePayloadVisitor : public boost::static_visitor<void> 
{
	RGWInstanceObjWatcher *inst_obj_watcher;
	uint64_t notify_id;
	uint64_t handle;

	RGWInstObjHandlePayloadVisitor(RGWInstanceObjWatcher *inst_obj_watcher_, uint64_t notify_id_, uint64_t handle_): 
		                        							inst_obj_watcher(inst_obj_watcher_), 
																					notify_id(notify_id_), 
																					handle(handle_)
	{
	}

	template <typename Payload>
	inline void operator()(const Payload &payload) const {
		C_RGWInstObjNotifyAck *ctx = new C_RGWInstObjNotifyAck(inst_obj_watcher, notify_id, handle);
		if (inst_obj_watcher->handle_payload(payload, ctx)) 
	  {
			ctx->complete(0);
		}
	}
};

class RGWBgtScheduler : public RGWInstanceObjWatcher, public Thread
{
public:
	RGWBgtScheduler(RGWInstanceObj* instobj, RGWRados* store, CephContext* cct);
	~RGWBgtScheduler() {}

	virtual int notify_active_changelog(const string& worker_name, const string& log_name, uint64_t timeout_ms);
	virtual int notify_dispatch_task(const string& worker_name, uint64_t task_id, const string& log_name, uint64_t start, uint32_t count, uint64_t timeout_ms);

	virtual bool handle_payload(const WorkerRegisterPayload& payload,	C_RGWInstObjNotifyAck *ctx);
	virtual bool handle_payload(const TaskFinishedPayload& payload,	C_RGWInstObjNotifyAck *ctx);


	virtual bool handle_payload(const ActiveChangeLogPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}
	virtual bool handle_payload(const DispatchTaskPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}
	virtual bool handle_payload(const RGWInstObjUnknownPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}

	string unique_change_log_id();
	
	void load_change_logs();
	void load_active_change_log();
	void load_workers();
	void load_tasks();

	void update_active_change_log();
	int set_active_change_log(RGWBgtLogTransInfo& log_trans_info);
	int set_change_logs(RGWBgtLogTransInfo& log_trans_info);
	int set_log_trans_info(RGWBgtLogTransInfo& log_trans_info);
	int get_log_trans_info(RGWBgtLogTransInfo& log_trans_info);
	void check_log_trans();

	int set_batch_task_info(RGWBgtBatchTaskInfo& batch_task_info);
	int get_batch_task_info(RGWBgtBatchTaskInfo& batch_task_info);
	void clear_task_entry(RGWBgtBatchTaskInfo& batch_task_info);
	void rm_log_entry(RGWBgtBatchTaskInfo& batch_task_info);
	void rm_change_log(RGWBgtBatchTaskInfo& batch_task_info);
	void dispatch_task(RGWBgtBatchTaskInfo& batch_task_info);
	void mk_task_entry_from_buf(bufferlist& bl, uint64_t rs_cnt, uint64_t& next_start, 
    	                              uint64_t& log_index, uint64_t& merge_size, 
    	                              uint64_t& task_id);
	void set_task_entries(RGWBgtBatchTaskInfo& batch_task_info);
	void create_task(RGWBgtBatchTaskInfo& batch_task_info);
	int update_task_entry(uint64_t task_id);
	void check_batch_task();

	//Thread
	void *entry();

	void start();
	void stop();	
protected:
  bool stopping;
  Mutex lock;
  Cond cond;
	Mutex data_lock;

	atomic64_t max_log_id;

	string active_change_log;
	std :: map < std :: string, RGWBgtChangeLogInfo> change_logs;
	std :: map < std :: string, RGWBgtWorkerInfo> workers;
	std :: map < uint64_t, RGWBgtTaskEntry> batch_tasks;
};

class RGWBgtWorker : public RGWInstanceObjWatcher, public Thread
{
	friend class ArchiveTask;
	
public:
	RGWBgtWorker(RGWInstanceObj* instobj, RGWRados* store, CephContext* cct);
	~RGWBgtWorker() 
	{
	}

	virtual int notify_register(uint64_t timeout_ms);
	virtual int notify_task_finished(uint64_t task_id, uint64_t timeout_ms);

	virtual bool handle_payload(const ActiveChangeLogPayload& payload,	C_RGWInstObjNotifyAck *ctx);
	virtual bool handle_payload(const DispatchTaskPayload& payload,	C_RGWInstObjNotifyAck *ctx);

	virtual bool handle_payload(const WorkerRegisterPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}
	virtual bool handle_payload(const TaskFinishedPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}
	virtual bool handle_payload(const RGWInstObjUnknownPayload& payload,	C_RGWInstObjNotifyAck *ctx){return false;}

	string unique_sfm_oid();
	int create_sfm_obj(RGWBgtTaskInfo& task_info);
	
	int set_task_info(RGWBgtTaskInfo& task_info);
	int get_task_info(RGWBgtTaskInfo& task_info);
	void report_task_finished(RGWBgtTaskInfo& task_info);
	int mk_sfm_index_from_change_log(bufferlist& bl, uint64_t rs_cnt, uint64_t& index_id);
	int create_sfm_index(RGWBgtTaskInfo& task_info);
	int mk_sfm_index_from_buf(bufferlist& bl, uint64_t rs_cnt, uint64_t& index_id);
	int load_sfm_index(RGWBgtTaskInfo & task_info);
	
	void sfm_flush_data(RGWBgtTaskInfo& task_info);
	void sfm_flush_index(RGWBgtTaskInfo& task_info);
	void set_sfm_result(RGWBgtTaskInfo& task_info);
	void process_merge_result(RGWBgtTaskInfo& task_info);
	void wait_merge(RGWBgtTaskInfo& task_info);
	void update_index(RGWBgtTaskInfo& task_info);
	void delete_data(RGWBgtTaskInfo& task_info);
	void load_task();
	void check_task();

	int write_change_log(RGWChangeLogEntry& log_entry);
	
	//Thread
	void *entry();

	void start();
	void stop();

protected:
  bool stopping;
  Mutex lock;
  Cond cond;	
	atomic64_t max_sfm_id;
	string active_change_log;
	RWLock update_change_log_lock;

	librados::IoCtx *m_sfm_io_ctx;
#if 0	
	libradosstriper::RadosStriper *m_sfm_striper;
#endif
	std :: map < string, librados::IoCtx> data_io_ctx;
	std :: map < string, librados::IoCtx> index_io_ctx;

	RGWBgtTaskSFMergeState merge_stage;
	std :: map < uint64_t, RGWSfmIndex> sfm_index;
	bufferlist sfm_bl;

	ArchiveTask m_archive_task;
};

#endif
