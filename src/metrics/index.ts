import { once } from 'events'
import express, { Application } from 'express'
import * as prometheus from 'prom-client'
import promBundle from 'express-prom-bundle'

const DEFAULT_PROM_BUNDLE_OPTS = {
  includeMethod: true,
  includePath: true,
  includeStatusCode: true,
  includeUp: true,
  promClient: {
    collectDefaultMetrics: {},
  },
  // Don't expose /metrics on main app - we'll use separate server
  autoregister: false,
}

export type MetricConfig =
  | {
      type: 'counter'
      name: string
      help: string
      labelNames?: string[]
    }
  | {
      type: 'histogram'
      name: string
      help: string
      labelNames?: string[]
      buckets?: number[]
    }
  | {
      type: 'gauge'
      name: string
      help: string
      labelNames?: string[]
    }
  | {
      type: 'summary'
      name: string
      help: string
      labelNames?: string[]
      percentiles?: number[]
    }

export type MetricInstance =
  | prometheus.Counter
  | prometheus.Histogram
  | prometheus.Gauge
  | prometheus.Summary

export class Metrics<
  T extends Record<string, MetricConfig> = Record<string, MetricConfig>,
> {
  private app: Application
  private registry: prometheus.Registry
  private metrics: { [K in keyof T]: MetricInstance }

  constructor(
    metricsConfig: T,
    options?: {
      prefix?: string
      collectDefaultMetrics?: boolean
      defaultLabels?: Record<string, string>
    }
  ) {
    const app = express()
    this.app = app

    const registry = new prometheus.Registry()
    this.registry = registry

    // Add add metrics route to express
    app.get('/metrics', async (_req, res) => {
      res.set('content-type', registry.contentType)
      res.end(await registry.metrics())
    })

    if (options?.collectDefaultMetrics !== false) {
      prometheus.collectDefaultMetrics({
        prefix: options?.prefix || '',
        register: this.registry,
        gcDurationBuckets: [0.001, 0.01, 0.1, 1, 2, 5],
      })
    }

    if (options?.defaultLabels) {
      registry.setDefaultLabels(options.defaultLabels)
    }

    this.metrics = {} as { [K in keyof T]: MetricInstance }

    for (const [key, config] of Object.entries(metricsConfig) as [
      keyof T,
      MetricConfig,
    ][]) {
      this.metrics[key] = this.createMetric(config, options?.prefix)
    }
  }

  registerExpressMetrics(app: Application, customOpts?: promBundle.Opts) {
    const opts = {
      ...DEFAULT_PROM_BUNDLE_OPTS,
      ...customOpts,
    }
    const middleware = promBundle(opts)
    app.use(middleware)
  }

  private createMetric(config: MetricConfig, prefix?: string): MetricInstance {
    const name = prefix ? `${prefix}${config.name}` : config.name
    const { help, labelNames } = config
    const registers = [this.registry]

    switch (config.type) {
      case 'counter':
        return new prometheus.Counter({
          name,
          labelNames,
          help,
          registers,
        })
      case 'histogram':
        return new prometheus.Histogram({
          name,
          help,
          labelNames,
          buckets: config.buckets,
          registers,
        })
      case 'gauge':
        return new prometheus.Gauge({
          name,
          help,
          labelNames,
          registers,
        })
      case 'summary':
        return new prometheus.Summary({
          name,
          help,
          labelNames,
          percentiles: config.percentiles,
          registers,
        })
      default:
        throw new Error(`Unknown metric type: ${(config as any).type}`)
    }
  }

  get<K extends keyof T>(metricName: K): MetricInstance {
    return this.metrics[metricName]
  }

  getCounter<K extends keyof T>(metricName: K): prometheus.Counter {
    return this.metrics[metricName] as prometheus.Counter
  }

  getHistogram<K extends keyof T>(metricName: K): prometheus.Histogram {
    return this.metrics[metricName] as prometheus.Histogram
  }

  getGauge<K extends keyof T>(metricName: K): prometheus.Gauge {
    return this.metrics[metricName] as prometheus.Gauge
  }

  getSummary<K extends keyof T>(metricName: K): prometheus.Summary {
    return this.metrics[metricName] as prometheus.Summary
  }

  async start(port: number) {
    const server = this.app.listen(port)
    await once(server, 'listening')
  }

  getRegistry(): prometheus.Registry {
    return this.registry
  }

  getApp(): Application {
    return this.app
  }
}
