#ifndef __HOLA_SYNC_QUEUE_H_INCLUDED__
#define __HOLA_SYNC_QUEUE_H_INCLUDED__

#include <map>
#include <list>
#include <memory>
#include <utility>
#include <vector>

namespace hola
{

template<typename T>
class SyncQueue
{
private:
    struct DataEntity{
        uint64_t time;
        int type;
        T data;
    };

public:
    class OutDataCallBack {
        public:
            virtual void OutData(T data) = 0;
    };

    SyncQueue()
        : m_maxTime(0)
        , m_syncTime(0)
        , m_curTime(0)
        , m_firstTime(0)
        , m_checkMax(false)
        , m_callBack(nullptr) {}

	SyncQueue(uint64_t maxTime, uint64_t syncTime, std::vector<int> typelist, OutDataCallBack *callback)
        : m_maxTime(maxTime)
        , m_syncTime(syncTime)
        , m_curTime(0)
        , m_firstTime(0)
        , m_checkMax(false)
        , m_callBack(callback) {
            for(auto it = typelist.begin(); it != typelist.end(); it++) {
                m_syncQueue.emplace(std::make_pair(*it, std::list<std::unique_ptr<DataEntity>>()));
            }
        }

    ~SyncQueue() {
        ForcePop();
    }

    void Initialize(uint64_t maxTime, uint64_t syncTime, std::vector<int> typelist, OutDataCallBack *callback) {
        m_maxTime = maxTime;
        m_syncTime = syncTime;
        m_callBack = callback;
        assert(m_maxTime > syncTime);
        for(auto it = typelist.begin(); it != typelist.end(); it++) {
            m_syncQueue.emplace(std::make_pair(*it, std::list<std::unique_ptr<DataEntity>>()));
        }
    }
    int Put(T &&dt, uint64_t time, int type) {
        std::unique_ptr<DataEntity> data(new DataEntity);
        data->data = std::move(dt);
        data->time = time;
        data->type = type;
        return Put(data);
    }

    int Put(T &dt, uint64_t time, int type) {
        std::unique_ptr<DataEntity> data(new DataEntity);
        data->data = dt;
        data->time = time;
        data->type = type;
        return Put(data);
    }

    int ForcePop() {
        int count = 0;
        while(!IsEmpty()) {
            m_firstTime += m_syncTime;
            count += CheckSent(m_firstTime);
        }
        m_firstTime = m_curTime = 0;
        return count;
    }

    int GetBufferCount() {
        int count = 0;
        for(auto its = m_syncQueue.begin(); its != m_syncQueue.end(); ++its) {
            count += its->second.size();
        }
        return count;
    }
private:
    int Put(std::unique_ptr<DataEntity> &data) {
        uint64_t time = data->time;
        if (time < m_maxTime || m_syncTime == 0) {
            return -1;
        }
        auto it = m_syncQueue.find(data->type);
        if (it == m_syncQueue.end()) {
            return -1;
        }

        if (m_firstTime == 0) {
            m_firstTime = time;
        }

        m_syncQueue[data->type].push_back(std::move(data));
        return CheckOut(time);
    }

    int CheckOut(uint64_t t) {
        int count = 0;
        if (t < m_firstTime) {
            count += CheckSent(m_firstTime);
            return count;
        }
        m_curTime = t - m_maxTime;
        while(m_firstTime <= m_curTime) {
            m_firstTime += m_syncTime;
            count += CheckSent(m_firstTime);
        }
        while (CanSent(m_firstTime)) {
            m_firstTime += m_syncTime;
            count += CheckSent(m_firstTime);
        }
        return count;
    }

    int CheckSent(uint64_t t) 
    {
        int count = 0;
        for(auto its = m_syncQueue.begin(); its != m_syncQueue.end(); ++its) {
            while(!its->second.empty() && its->second.front()->time < t) {
                if (m_callBack) {
                    m_callBack->OutData(std::move(its->second.front()->data));
                }
                count ++;
                its->second.pop_front();
            }
        }
        return count;
    }


    bool CanSent(uint64_t t) {
        t += m_syncTime;
        for(auto its = m_syncQueue.begin(); its != m_syncQueue.end(); ++its) {
            if (its->second.empty()) {
                return false;
            }
        }
        for(auto its = m_syncQueue.begin(); its != m_syncQueue.end(); ++its) {
            auto &front = its->second.front();
            if (front->time > t) {
                return false;
            }
        }
        return true;
    }

    bool IsEmpty() {
        for(auto its = m_syncQueue.begin(); its != m_syncQueue.end(); ++its) {
            if (!its->second.empty()) {
                return false;
            }
        }
        return true;
    }

private:
    std::map<int, std::list<std::unique_ptr<DataEntity>>> m_syncQueue;
    uint64_t m_maxTime;
    uint64_t m_syncTime;
    uint64_t m_curTime;
    uint64_t m_firstTime;
    bool m_checkMax;
    OutDataCallBack *m_callBack;
}; // class SyncQueue

} // namespace hola

#endif // ifndef __HOLA_SYNC_QUEUE_H_INCLUDED__

