import axios, { AxiosRequestConfig } from 'axios'
import _ from 'lodash'
import bluebird from 'bluebird'
import crypto from 'crypto';
import path from "path";
import chalk from 'chalk';
import cron from 'node-cron';
import { env } from 'process';
import dayjs from 'dayjs';

let isInit = false

let globalOpt: Required<initOpt> = {
    DmlConsulUrl: process.env.DML_CONSUL_URL || '',
    Namespace: process.env.DML_CONSUL_NAMESPACE || 'default',
    Token: process.env.DML_CONSUL_TOKEN || '',
    CronExpression: env.DML_CONSUL_CRON_EXPRESSION || '',
    Urls: [],
    Timeout: 2 * 1000,
    MultipleCount: 1,
    SdkAgent: "nodejsSdk",
    NamespaceDelimiter: '_',
    CheckServiceName: true,
    Throw: false
}

export async function init(customOpt: initOpt) {
    if (isInit){
        return
    }
    globalOpt = mergeOpt(customOpt)
    if (globalOpt.DmlConsulUrl != "") {
        await ResolveUrls()
    }
    if (globalOpt.MultipleCount > globalOpt.Urls.length) {
        throw new Error('MultipleCount 不能大于 Urls 长度')
    }
    log('Namespace:     ', globalOpt.Namespace)
    log('Domain:        ', globalOpt.DmlConsulUrl)
    log('Urls:          ', globalOpt.Urls)
    log('Timeout:       ', globalOpt.Timeout)
    log('MultipleCount: ', globalOpt.MultipleCount)
    isInit = true
    if (customOpt.CronExpression && globalOpt.DmlConsulUrl != "") {
        if (!cron.validate(customOpt.CronExpression)) {
            throw new Error(`CronExpression Error: ${customOpt.CronExpression}`)
        }
        const task = cron.schedule(customOpt.CronExpression, async () => {
            await ResolveUrls()
        })
        task.start()
        log('CronStart:     ', globalOpt.CronExpression)
    }
}



export async function ResolveUrls(): Promise<string[]> {
    const res = await axios({
        baseURL: globalOpt.DmlConsulUrl,
        url: `v1/catalog/nodes`,
        method: 'GET',
    })
    let urls = []
    for (const v of res.data) {
        urls.push(`http://${v.Address}:8500`)
    }
    urls = _.union(urls)
    globalOpt.Urls = urls
    return urls
}

function log(...str: any[]) {
    console.info(chalk.magenta('dml-consul: '), chalk.cyan(str.join('')))
}

function mergeOpt(customOpt?: initOpt): Required<initOpt> {
    const opt = _.merge(globalOpt, customOpt)

    if (opt.Namespace.includes('.')) {
        throw new Error('Namespace cant contain .')
    }
    if (opt.NamespaceDelimiter && opt.Namespace.includes(opt.NamespaceDelimiter)) {
        throw new Error(`Namespace cant contain ${opt.NamespaceDelimiter}`)
    }
    return opt
}

function addNs(str: string, delimiter: string, opt: initOpt): string {
    str = str.toLowerCase()
    if (opt.Namespace == "" || str.substr(0, opt.Namespace.length + 1) === `${opt.Namespace}${delimiter}`) {
        return str
    }
    return `${opt.Namespace}${delimiter}${str}`
}

function delNs(str: string, delimiter: string, opt: initOpt): string {
    str = str.toLowerCase()
    if (str.substr(0, opt.Namespace.length + 1) === `${opt.Namespace}${delimiter}`) {
        return str.substr(opt.Namespace.length + 1)
    }
    return str
}

function baseRequest(axiosEntity: AxiosRequestConfig, opt: Required<initOpt>): Promise<any>[] {
    if (!isInit) {
        throw new Error("Must await init()!")
    }
    const promiseArr = [];
    for (const url of opt.Urls) {
        promiseArr.push(axios({
            baseURL: url,
            headers: {
                'X-Consul-Token': opt.Token,
            },
            ...axiosEntity,
            timeout: opt.Timeout,

        }).then(v => { return v }).catch(e => { throw resultErr(_.get(e, 'response.data') || e.message, e) }));
    }
    return promiseArr;
}

function resultOk(data?: any, message = 'ok'): commonRes {
    return {
        RetCode: 0,
        Message: message,
        Data: data,
    }
}

function resultErr(message: string, stack?: any): commonRes {
    return {
        RetCode: -1,
        Message: message,
        Stack: stack,
    }
}

interface commonRes {
    RetCode: number,
    Data?: any,
    Message: string,
    Stack?: any,
}

/**
 * 发现多个服务
 * @param params 
 * @returns 
 * @link https://www.consul.io/api-docs/agent/service#list-services
 */
export async function ServiceList(params: ServiceListReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        if (params.NameFilter) {
            params.NameFilter = params.NameFilter.toLowerCase()
        }
        const res = await bluebird.some(baseRequest({
            url: `/v1/agent/services`,
            method: 'get',
            params: {
                filter: `Service matches "${opt.Namespace}${opt.NamespaceDelimiter}"` + (params.NameFilter ? ` and Service matches "${params.NameFilter}"` : '')
            }
        }, opt), opt.MultipleCount)
        let serviceIdObj: { [key: string]: any } = {}
        for (const { data } of res) {
            serviceIdObj = Object.assign(serviceIdObj, data)
        }

        if (params.Health) {
            const healthList = await bluebird.any(baseRequest({
                url: `/v1/health/state/critical`,
                method: 'get',
                params: {
                    filter: `ServiceName matches "${opt.Namespace}${opt.NamespaceDelimiter}"`
                }
            }, opt))
            for (const { ServiceID } of healthList.data) {
                serviceIdObj[ServiceID] = undefined
            }
        }
        const result: { [key: string]: any } = {}
        for (const key in serviceIdObj) {
            const info = serviceIdObj[key]
            if (info) {
                result[delNs(info.Service, opt.NamespaceDelimiter, opt)] = _.concat(result[delNs(info.Service, opt.NamespaceDelimiter, opt)] || [], _.pick(info, ['Address', 'Port', 'ID']))
            }
        }
        return resultOk(result)
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res 
    }
}

/**
 * 发现单个服务
 * @param params 
 * @returns
 * @link https://www.consul.io/api-docs/health#list-checks-in-state
 * @link https://www.consul.io/api-docs/agent/service#get-service-configuration
 */
export async function ServiceInfo(params: ServiceInfoReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        params.Name = params.Name.toLowerCase()
        const [res, healthList] = await bluebird.all([bluebird.any(baseRequest({
            url: `/v1/catalog/service/${encodeURIComponent(addNs(params.Name, opt.NamespaceDelimiter, opt))}`,
            method: 'get',
        }, opt)), bluebird.any(baseRequest({
            url: `/v1/health/state/any`,
            method: 'get',
            params: {
                filter: `ServiceName == "${addNs(params.Name, opt.NamespaceDelimiter, opt)}"`
            }
        }, opt))])

        const criticalObj: { [key: string]: any } = {}
        const checkObj: { [key: string]: any } = {}

        for (const v of healthList.data) {
            checkObj[v.ServiceID] = _.pick(v, ['Output', 'Type', 'Status'])
            if (params.Health && v.Status === 'critical') criticalObj[v.ServiceID] = true
        }

        return resultOk(
            _.filter(
                _.uniqBy(
                    res.data.map((v: any) => {
                        return {
                            Address: v.ServiceAddress,
                            Port: v.ServicePort,
                            ID: v.ServiceID,
                            Tags: v.ServiceTags,
                            Check: checkObj[v.ServiceID] || {}
                        }
                    }
                    ), 'ID')
                , function (o: any) { return !criticalObj[o.ID] })
        )
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 服务注册
 * @param params 
 * @returns 
 * @link https://www.consul.io/api-docs/agent/service#register-service
 */
export async function ServiceAdd(params: ServiceAddReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        if (!params.Address || !params.Port || !params.Name || !params.Check) {
            return resultErr('Lost params [Address、Port、Name、Check]')
        }
        if (globalOpt.CheckServiceName && !/^[a-zA-Z0-9\_\-]{3,}$/.test(params.Name)) {
            return resultErr('Name must reg /^[a-zA-Z0-9\_\-]{3,}$/')
        }
        params.Name = params.Name.toLowerCase()
        params.Name = addNs(params.Name, opt.NamespaceDelimiter, opt)
        params.ID = `${params.Name}${opt.NamespaceDelimiter}` + crypto.createHmac('md5', '').update(`${params.Address}_${params.Port}`).digest('base64').slice(0, 16).toUpperCase().replace(/\+/g, 'I').replace(/\//g, 'J').replace(/\=/g, 'k')
        if (!params.Tags) {
            params.Tags = []
        }
        params.Tags = [params.Name, params.ID, `${params.Address}:${params.Port}`, opt.SdkAgent || "", dayjs().format("YYYY-MM-DD HH:mm:ss"), ...params.Tags]
        if (!params.Check.Interval) {
            params.Check.Interval = '1s'
        }
        if (!params.Check.Status) {
            params.Check.Status = 'passing'
        }

        console.log()
        log(chalk.yellow("Register Service..."))
        log('ID:            ', params.ID)
        log('Name:          ', params.Name)
        log('Address:       ', params.Address)
        log('Port:          ', params.Port)
        log('Check.Interval:', params.Check.Interval)
        if (params.Check.Http) {
            log('Check.Http:    ', params.Check.Http)
        }
        if (params.Check.TCP) {
            log('Check.TCP:     ', params.Check.TCP)
        }

        if (params.Address.includes('127.0.0.1') || params.Address.includes('localhost')) {
            log(chalk.red('Address 为本地地址,取消注册!'))
            return resultOk(_.pick(params, ['ID', 'Name', 'Address', 'Port']), 'Address 为本地地址,取消注册')
        }

        await bluebird.some(baseRequest({
            url: `/v1/agent/service/register`,
            method: 'put',
            data: {
                ...params,
            }
        }, opt), 1)

        log(chalk.blue('\nService Register Success!'))
        return resultOk(_.pick(params, ['ID', 'Name', 'Address', 'Port']))
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 服务注销
 * @param params 
 * @returns 
 * @link https://www.consul.io/api-docs/catalog#list-services
 * @link https://www.consul.io/api-docs/agent/service#deregister-service
 */
export async function ServiceDel(params: ServiceDelReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    let IdArr = [];
    try {
        if (params.ID) {
            IdArr.push(params.ID);
        } else if (params.Name) {
            const service = await bluebird.any(baseRequest({
                url: `/v1/catalog/service/${encodeURIComponent(addNs(params.Name, opt.NamespaceDelimiter, opt))}`,
                method: 'get',
            }, opt))
            service.data.forEach((e: { ServiceID: any; }) => {
                if (e.ServiceID) {
                    IdArr.push(e.ServiceID)
                }
            });
        }
        if (IdArr.length === 0) {
            return resultErr('没有找到匹配的服务')
        }
        IdArr = _.union(IdArr)
        for (const id of IdArr) {
            await bluebird.any(baseRequest({
                url: `/v1/agent/service/deregister/${encodeURIComponent(id)}`,
                method: 'put',
            }, opt));
        }
        return resultOk(IdArr)
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 获取KV配置信息
 * @param params 
 * @link https://www.consul.io/api-docs/kv#read-key
 */
export async function KvInfo(params: GetKvReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        const { data } = await bluebird.any(baseRequest({
            url: path.join('/v1/kv/', encodeURIComponent(addNs(params.Key, '/', opt))),
            method: 'get',
        }, opt))
        let consulData = Buffer.from(data[0].Value, 'base64').toString();
        if (params.JsonParse) {
            consulData = JSON.parse(consulData)
        }
        return resultOk({
            ..._.omit(data[0], ['Key', 'Flags']),
            Value: consulData
        })
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 递归获取多个KV配置信息
 * @param params 
 * @returns
 * @link https://www.consul.io/api-docs/kv#read-key
 */
export async function KvList(params: GetKvReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        const { data } = await bluebird.any(baseRequest({
            url: path.join('/v1/kv/', encodeURIComponent(addNs(params.Key, '/', opt))),
            method: 'get',
            params: {
                recurse: true
            }
        }, opt))
        const result: { [key: string]: any } = {}
        for (const { Key, Value } of data) {
            if (![null, '', undefined].includes(Value)) {
                let consulData = Buffer.from(Value, 'base64').toString();
                if (params.JsonParse) {
                    consulData = JSON.parse(consulData)
                }
                result[delNs(Key, '/', opt)] = consulData
            }
        }
        return resultOk(result)
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 新增、更新KV配置
 * @param params 
 * @returns 
 * @link https://www.consul.io/api-docs/kv#create-update-key
 */
export async function KvUpSert(params: KvUpSertReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        if (!params.Key || params.Value === undefined) {
            return resultErr('Lost params [Key、Value]')
        }
        if (_.isNumber(params.Value)) {
            params.Value = Number(params.Value).toString()
        }
        const res = await bluebird.any(baseRequest({
            url: path.join('/v1/kv/', encodeURIComponent(addNs(params.Key, '/', opt))),
            method: 'put',
            params: {
                cas: params.Cas
            },
            data: params.Value,
        }, opt))
        if (res.data !== true) {
            return resultErr('更新失败')
        }
        return resultOk({})
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 删除KV
 * @param params 
 * @link https://www.consul.io/api-docs/kv#delete-key
 */
export async function KvDel(params: KvDelReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        const res = await bluebird.any(baseRequest({
            url: path.join('/v1/kv/', encodeURIComponent(addNs(params.Key, '/', opt))),
            method: 'delete',
            params: {
                cas: params.Cas,
            },
        }, opt))
        if (res.data !== true) {
            return resultErr('删除失败')
        }
        return resultOk({})
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 批量删除KV
 * @param params 
 * @link https://www.consul.io/api-docs/kv#delete-key
 */
export async function KvTreeDel(params: KvTreeDelReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        const res = await bluebird.any(baseRequest({
            url: path.join('/v1/kv/', encodeURIComponent(addNs(params.Key, '/', opt))),
            method: 'delete',
            params: {
                recurse: true
            },
        }, opt))
        if (res.data !== true) {
            return resultErr('删除失败')
        }
        return resultOk({})
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}

/**
 * 获取检查列表
 * @param params
 * @returns
 * @link https://www.consul.io/api-docs/health#list-checks-in-state
 */
export async function CheckList(params: CheckListReq, customOpt?: initOpt): Promise<commonRes> {
    const opt = mergeOpt(customOpt)

    try {
        if (params.NameFilter) {
            params.NameFilter = params.NameFilter.toLowerCase()
        }
        if (!['any', 'warning', 'critical', 'passing'].includes(params.State)) {
            params.State = 'any'
        }
        const res = await bluebird.any(baseRequest({
            url: `/v1/health/state/${params.State}`,
            method: 'get',
            params: {
                filter: `ServiceName matches "${opt.Namespace}${opt.NamespaceDelimiter}"` + (params.NameFilter ? ` and ServiceName matches "${params.NameFilter}"` : '')
            },
        }, opt))
        const result: { [key: string]: any } = {}

        for (const v of res.data) {
            if (v.ServiceName !== '') {
                result[delNs(v.ServiceName, opt.NamespaceDelimiter, opt)] = _.concat(result[delNs(v.ServiceName, opt.NamespaceDelimiter, opt)] || [], {
                    ID: v.ServiceID,
                    Tags: v.ServiceTags,
                    Node: v.Node,
                    Output: v.Output,
                    Type: v.Type,
                    Status: v.Status,
                    CheckID: v.CheckID,
                })
            }
        }
        return resultOk(result)
    } catch (e: any) {
        const res = e[0] || resultErr(e.message, e)
        if (globalOpt.Throw){
            throw res.Message
        }
        return res
    }
}


interface initOpt {
    DmlConsulUrl?: string //consul 地址
    CronExpression?: string // 定时刷新consul nodes
    Namespace: string // 命名空间
    Token: string // consul 访问token
    Urls?: string[] // consul nodes
    Timeout?: number // 访问超时时间
    MultipleCount?: number // 最小注册量
    SdkAgent?: string // 标识
    NamespaceDelimiter?: string // 命名空间分割符
    CheckServiceName?: boolean // 检查服务名称是否合法, 用于服务发现
    Throw?: boolean
}
interface ServiceListReq {
    NameFilter?: string
    Health?: boolean
}
interface CheckListReq {
    NameFilter?: string,
    State: string
}

interface KvDelReq {
    Key: string
    Cas?: number
}
interface KvTreeDelReq {
    Key: string
}

interface KvUpSertReq {
    Key: string
    Value: any
    Cas?: number
}

interface ServiceInfoReq {
    Name: string
    Health?: boolean
}

interface GetKvReq {
    Key: string
    JsonParse?: boolean
}

interface ServiceDelReq {
    ID?: string
    Name?: string,
}

interface ServiceAddReq {
    Name: string
    Address: string
    Port: number
    EnableTagOverride?: boolean
    Check?: checkReq
    ID?: string
    Tags?: string[]
}

interface checkReq {
    Http?: string
    TCP?: string
    Status?: string
    Interval?: string
    Timeout?: string
    DeregisterCriticalServiceAfter?: string
}