

import cluster from "cluster";
import { pid } from "process";
import { Server, Socket } from "socket.io";
import { fileURLToPath } from "url";
const { setupMaster, setupWorker } = await import("@socket.io/sticky");
const { createAdapter, setupPrimary } = await import("@socket.io/cluster-adapter");

import { createRequire } from "module";
const require = createRequire(import.meta.url)
const env = require("$/server/env.json")

const path = (await import("path")).default;
const fs = (await import("fs")).default;
const log_util = await import("$/server/utils/log/index.js");
const log = await log_util.local_log_decorator("channels_builder", "yellow", true, "Info", false);

const directory_alias_suffix_regx = RegExp(env.channels.alias_suffix_regx || "\\.directory_alias\\.js$");
const channel_suffix_regx = RegExp(env.channels.channel_suffix_regx || "\\.channel\\.(?:js|ts)$");
const middleware_suffix_regx = RegExp(env.channels.middleware_suffix_regx || "\\.middleware\\.(?:js|ts)$");
const description_suffix_regx = RegExp(env.channels.description_suffix_regx || "\\.description\\.[a-zA-Z]{1,10}$");

const aliases: Function[] = [];

const src_path = path.resolve(path.join(path.dirname(fileURLToPath(import.meta.url)), "../../."));

const resolve_ts = (path: string) => {
    if (path.endsWith(".ts")) {
        path = path.replace(/\.ts$/, ".js");
    }
    return path;
};

export type Respond = (response: any) => void;

export type ChannelHandlerBeforeMounted = (
    socket: Socket,
) => null | undefined | void | string | boolean | Promise<null | undefined | void | string | boolean>;
export type ChannelHandlerBuilder = (
    socket: Socket,
) => ChannelHandler | null | void | undefined | string | Promise<ChannelHandler | string | void | null | undefined>;
export type ChannelHandlerMounted = (
    socket: Socket,
) => null | undefined | void | string | Promise<null | undefined | void | string>;

export type ChannelHandlerFunction = (body: any, respond: Respond | undefined, ev: string) => any;
export type _ChannelHandler = ChannelHandler[];
export type ChannelHandler = ChannelHandlerFunction | _ChannelHandler;
export type ChannelDirectoryAliasDefaultExport = {
    target_directory: string;
    include_target_middlewares: boolean;
};

export async function get_middlewares_array(
    current_channels_directory: string,
): Promise<
    { default?: ChannelHandlerBuilder; mounted?: ChannelHandlerMounted; before_mounted?: ChannelHandlerBeforeMounted }[]
> {
    const content = fs.readdirSync(current_channels_directory);
    const middlewares = await Promise.all(
        content
            .filter((f) => {
                const file_stats = fs.statSync(path.join(current_channels_directory, f));
                return file_stats.isFile() && !!f.match(middleware_suffix_regx);
            })
            .map(async (f) => {
                let full_path = path.join(current_channels_directory, f);

                return await import(resolve_ts(full_path));
            }),
    );
    return middlewares;
}

export const handlers: {
    path: string;

    before_mounted_middlewares: {
        middleware: ChannelHandlerBeforeMounted[];
        path: string;
    }[];
    middlewares: {
        middleware: ChannelHandlerBuilder[];
        path: string;
    }[];
    mounted_middlewares: {
        middleware: ChannelHandlerMounted[];
        path: string;
    }[];

    handler?: ChannelHandlerBuilder;
    mounted?: ChannelHandlerMounted;
    before_mounted?: ChannelHandlerBeforeMounted;
}[] = [];

export const perform = async (body: any, respond: Respond | undefined, handler: ChannelHandler, ev: string) => {
    if (typeof handler == "function") {
        await handler(body, respond, ev);
    } else if (Array.isArray(handler)) {
        for (const sub_handler of handler) {
            await perform(body, respond, sub_handler, ev);
        }
    }
};

export const register_socket = async (socket: Socket) => {
    console.log("socket connection", socket.id)
    try {
        let has_handlers = false;
        const applied_before_mounted_middlewares: {
            path: string;
            accepted: boolean;
        }[] = [];
        const applied_middlewares: {
            path: string;
            processed: any[];
            accepted: boolean;
        }[] = [];
        const applied_mounted_middlewares: string[] = [];
        socket.data.access_map = [];
        for (const handler of handlers) {
            let all_before_mounted_middlewares_accepted = true;
            if (handler.before_mounted_middlewares?.length) {
                for (const before_mounted_middleware of handler.before_mounted_middlewares) {
                    const found_applied = applied_before_mounted_middlewares.find(
                        (abmm) => abmm.path == before_mounted_middleware.path,
                    );
                    if (!found_applied) {
                        let before_mounted_middlewares_accepted = true;
                        for (const before_middleware of before_mounted_middleware.middleware) {
                            const middleware_accepted = await before_middleware(socket);

                            if (middleware_accepted === false || typeof middleware_accepted == "string") {
                                all_before_mounted_middlewares_accepted = false;
                                before_mounted_middlewares_accepted = false;

                                socket.data.access_map.push({
                                    accessible: false,
                                    path: handler.path,
                                    before_mounted_middleware_path: before_mounted_middleware.path,
                                    rejection_reason:
                                        typeof middleware_accepted == "string"
                                            ? middleware_accepted
                                            : "one of the middlewares before mounted handlers rejected",
                                });
                                break;
                            }
                        }
                        applied_before_mounted_middlewares.push({
                            path: before_mounted_middleware.path,
                            accepted: before_mounted_middlewares_accepted,
                        });

                        if (all_before_mounted_middlewares_accepted === false) {
                            break;
                        }
                    } else {
                        all_before_mounted_middlewares_accepted = found_applied.accepted;

                        if (all_before_mounted_middlewares_accepted === false) {
                            socket.data.access_map.push({
                                accessible: false,
                                path: handler.path,
                                rejection_reason:
                                    socket.data.access_map?.find((a) => {
                                        return a?.before_mounted_middleware_path == found_applied.path;
                                    })?.rejection_reason || "one of the middlewares before mounted handlers rejected",
                            });
                            break;
                        }
                    }
                }
            }
            if (!all_before_mounted_middlewares_accepted) {
                continue;
            }

            if (handler.before_mounted) {
                const accepted = await handler.before_mounted?.(socket);
                if (accepted === false || typeof accepted == "string") {
                    socket.data.access_map.push({
                        path: handler.path,
                        accessible: false,
                        rejection_reason:
                            typeof accepted == "string"
                                ? accepted
                                : "this event not accessible, rejected on event before mounted",
                    });
                    continue;
                }
            }

            const main_handlers = await handler.handler?.(socket);
            if (main_handlers && typeof main_handlers != "string") {
                const handlers: ChannelHandler[] = [];
                let all_middlewares_accepted = true;
                for (const middleware of handler.middlewares) {
                    let middleware_handler: any[] = [];
                    const found_applied = applied_middlewares.find((am) => am.path == middleware.path);
                    if (!found_applied) {
                        const processed = [] as any[];

                        for (const middleware_handler of middleware.middleware) {
                            const processed_middleware_handler = await middleware_handler(socket);
                            if (!!processed_middleware_handler && typeof processed_middleware_handler != "string") {
                                processed.push(processed_middleware_handler);
                            } else {
                                socket.data.access_map.push({
                                    accessible: false,
                                    path: handler.path,
                                    middleware_path: middleware.path,
                                    rejection_reason:
                                        typeof processed_middleware_handler == "string"
                                            ? processed_middleware_handler
                                            : "one of the middlewares handlers rejected",
                                });
                                all_middlewares_accepted = false;
                                break;
                            }
                        }
                        if (!all_middlewares_accepted) {
                            applied_middlewares.push({
                                path: middleware.path,
                                processed: [],
                                accepted: false,
                            });
                            break;
                        }

                        applied_middlewares.push({
                            path: middleware.path,
                            processed: processed,
                            accepted: true,
                        });
                        middleware_handler = processed;
                    } else {
                        if (found_applied.accepted) {
                            middleware_handler = found_applied.processed;
                        } else {
                            all_middlewares_accepted = false;
                            socket.data.access_map.push({
                                accessible: false,
                                path: handler.path,
                                rejection_reason:
                                    socket.data.access_map?.find((a) => {
                                        return a?.middleware_path == found_applied.path;
                                    })?.rejection_reason || "one of the middlewares handlers rejected",
                            });
                            break;
                        }
                    }
                    handlers.push(...middleware_handler);
                }

                if (!all_middlewares_accepted) {
                    continue;
                }
                handlers.push(main_handlers);
                has_handlers = true;
                if (!handler.path.endsWith("/")) {
                    handler.path = handler.path + "/";
                }
                socket.on(handler.path, async (body: any, cb?: Respond) => {
                    try {
                        await perform(body, cb, handlers, handler.path);
                    } catch (error: any) {
                        console.log(error);
                        log.error("Channel Error", handler.path, error);
                        if (cb) {
                            if (error?.http_error) {
                                error = {
                                    message: error.message,
                                    response: {
                                        data: error,
                                    },
                                };
                            }
                            if (error?.error) {
                                cb({
                                    error: error.error,
                                });
                            } else {
                                cb({
                                    error: error,
                                });
                            }
                        }
                    }
                });
            } else {
                socket.data.access_map.push({
                    path: handler.path,
                    accessible: false,
                    rejection_reason: typeof main_handlers == "string" ? main_handlers : "this event not accessible",
                });
            }

            if (handler.mounted) {
                await handler.mounted(socket);
            }

            if (handler.mounted_middlewares?.length) {
                for (const mounted_middleware of handler.mounted_middlewares) {
                    if (!applied_mounted_middlewares.find((path) => path == mounted_middleware.path)) {
                        for (const middleware of mounted_middleware.middleware) {
                            await middleware(socket);
                        }
                        applied_mounted_middlewares.push(mounted_middleware.path);
                    }
                }
            }
        }

        socket.use(([event, ...args], next) => {
            const cb = args?.at?.(-1);
            const found_event = handlers.find((h) => {
                return h.path == event;
            });
            console.log("incoming event", event, found_event?.path ? "(event found)" : "(event not found)")
            if (typeof cb == "function") {
                

                if (!found_event) {
                    console.log(`event not found`, event);
                    cb({
                        // status_code: env.response.status_codes.not_found,
                        error: {
                            msg: "event not found",
                        },
                    });
                    return
                }
                const found_accessibility = socket.data.access_map.find((a) => a.path == event);
                if (found_accessibility) {
                    if (found_accessibility.accessible === false) {
                        cb({
                            // status_code: env.response.status_codes.action_not_authorized,
                            error: {
                                msg: found_accessibility.rejection_reason || "event not accessible",
                            },
                        });
                    } else {
                        next();
                    }
                } else {
                    next();
                }
            } else {
                next();
            }
        });

        if (!has_handlers) {
            socket.disconnect();
        }
    } catch (error: any) {
        log.error("Channel Error", error);
        if (socket.connected) {
            if (error.error) {
                socket.emit("error", {
                    error: error.error,
                    status_code: error?.status_code,
                });
            } else {
                socket.emit("error", {
                    error: error,
                });
            }
            socket.disconnect();
        }
    }
};

export let io: Server;

export const emit = (event: string, ...args: any[]) => io.emit(event, ...args);

export const emit_to_room = (room: string, event: string, ...arg: any[]) => {
    io.to(room).emit(event, ...arg);
};

export const run = (httpServer) => {
    io = new Server(httpServer, {
        path: env.channels.sockets_prefix,
        cors: {
            origin: "*",
            methods: ["GET", "POST"],
        },
        perMessageDeflate: {
            threshold: 1024, // Minimum size in bytes before compressing
            zlibDeflateOptions: {
                // Options for zlib's deflate
                chunkSize: 1024,
            },
            zlibInflateOptions: {
                // Options for zlib's inflate
                chunkSize: 10 * 1024,
            },
        },
    });
    io.on("connection", async (socket) => {
        await register_socket(socket);
    });
};
export const run_threaded = (httpServer) => {
    if (cluster.isPrimary) {
        setupMaster(httpServer, {
            loadBalancingMethod: "least-connection",
        });
        setupPrimary();
        log("setup primary cluster done");
    } else {
        io = new Server(httpServer, {
            path: env.channels.sockets_prefix,
            cors: {
                origin: "*",
                methods: ["GET", "POST"],
            },
            perMessageDeflate: {
                threshold: 1024, // Minimum size in bytes before compressing
                zlibDeflateOptions: {
                    // Options for zlib's deflate
                    chunkSize: 1024,
                },
                zlibInflateOptions: {
                    // Options for zlib's inflate
                    chunkSize: 10 * 1024,
                },
            },
        });
        io.adapter(createAdapter());

        setupWorker(io);

        io.on("connection", async (socket) => {
            console.log("connected", socket.id, pid);
            await register_socket(socket);
        });
    }
};

export default async function build_channelling(
    app: any,
    current_channels_directory = path.join(src_path, env.channels.channels_directory),
    root = true,
    full_prefix = "/",
    provided_before_mounted_middlewares: {
        middleware: ChannelHandlerBeforeMounted[];
        path: string;
    }[] = [],
    provided_middlewares: {
        middleware: ChannelHandlerBuilder[];
        path: string;
    }[] = [],
    provided_mounted_middlewares: {
        middleware: ChannelHandlerMounted[];
        path: string;
    }[] = [],
) {
    const content = fs.readdirSync(current_channels_directory);
    root && log("Building Channels", current_channels_directory);

    const before_mounted_middlewares = [...(provided_before_mounted_middlewares || [])];
    const middlewares = [...(provided_middlewares || [])];
    const mounted_middlewares = [...(provided_mounted_middlewares || [])];

    const loaded_middlewares = await get_middlewares_array(current_channels_directory);

    for (const middleware of loaded_middlewares || []) {
        if (middleware.before_mounted) {
            const found_before_mounted_middleware = before_mounted_middlewares.find((pbmm) => pbmm.path == full_prefix);
            if (found_before_mounted_middleware) {
                found_before_mounted_middleware.middleware.push(middleware.before_mounted);
            } else {
                before_mounted_middlewares.push({
                    middleware: [middleware.before_mounted],
                    path: full_prefix,
                });
            }
        }

        if (middleware.default) {
            const found_middleware = middlewares.find((pm) => pm.path == full_prefix);
            if (found_middleware) {
                found_middleware.middleware.push(middleware.default);
            } else {
                middlewares.push({
                    middleware: [middleware.default],
                    path: full_prefix,
                });
            }
        }

        if (middleware.mounted) {
            const found_mounted_middleware = mounted_middlewares.find((pbmm) => pbmm.path == full_prefix);
            if (found_mounted_middleware) {
                found_mounted_middleware.middleware.push(middleware.mounted);
            } else {
                mounted_middlewares.push({
                    middleware: [middleware.mounted],
                    path: full_prefix,
                });
            }
        }
    }

    for (const item of content) {
        const item_stat = fs.statSync(path.join(current_channels_directory, item));

        if (item_stat.isDirectory()) {
            await build_channelling(
                app,
                path.join(current_channels_directory, item),
                false,
                path.join(full_prefix, item),
                before_mounted_middlewares,
                middlewares,
                mounted_middlewares,
            );
        } else {
            const channel_match = item.match(channel_suffix_regx);
            if (!!channel_match) {
                // process.env.NODE_ENV !== "test" && console.log("Route", current_channels_directory);
                const router_name = item.slice(0, item.indexOf(channel_match[0]));
                if (router_name == "index") {
                    const handler_path = resolve_ts(path.join(current_channels_directory, item));
                    const { default: handler, mounted, before_mounted } = await import(handler_path);
                    const router_description_regx = RegExp(
                        `${router_name}${description_suffix_regx.toString().slice(1, -1)}`,
                    );
                    const router_description_file = content.filter((el) => !!el.match(router_description_regx))[0];
                    handlers.push({
                        path: full_prefix,

                        before_mounted,
                        handler,
                        mounted,

                        middlewares: [...middlewares],
                        mounted_middlewares: [...mounted_middlewares],
                        before_mounted_middlewares: [...before_mounted_middlewares],
                    });
                    router_description_file &&
                        app.get(path.join("channelling", full_prefix, "describe"), async (request, response, next) => {
                            try {
                                response.sendFile(
                                    path.join(current_channels_directory, router_description_file),
                                    (error) => {
                                        !!error && next(error);
                                    },
                                );
                            } catch (error) {
                                next(error);
                            }
                        });
                } else {
                    const handler_path = resolve_ts(path.join(current_channels_directory, item));
                    const { default: handler, mounted, before_mounted } = await import(handler_path);
                    const router_description_regx = RegExp(
                        `${router_name}${description_suffix_regx.toString().slice(1, -1)}`,
                    );
                    const router_description_file = content.filter((el) => !!el.match(router_description_regx))[0];
                    const channel_path = path.join(full_prefix, router_name);
                    handlers.push({
                        path: channel_path,

                        mounted,
                        handler,
                        before_mounted,

                        middlewares: [...middlewares],
                        mounted_middlewares: [...mounted_middlewares],
                        before_mounted_middlewares: [...before_mounted_middlewares],
                    });
                    router_description_file &&
                        app.get(path.join("channelling", channel_path, "describe"), async (request, response, next) => {
                            try {
                                response.sendFile(
                                    path.join(current_channels_directory, router_description_file),
                                    (error) => {
                                        !!error && next(error);
                                    },
                                );
                            } catch (error) {
                                next(error);
                            }
                        });
                }
            } else {
                const directory_alias_match = item.match(directory_alias_suffix_regx);
                if (!!directory_alias_match) {
                    aliases.push(async () => {
                        const router_alias: ChannelDirectoryAliasDefaultExport = (
                            await import(path.join(current_channels_directory, item))
                        ).default;
                        const router_name = item.slice(0, item.indexOf(directory_alias_match[0]));
                        const aliases = handlers
                            .filter((h) => {
                                return h.path.startsWith(router_alias.target_directory);
                            })
                            .map((h) => {
                                const new_handler = { ...h };
                                new_handler.path = new_handler.path.replace(
                                    RegExp(`^${router_alias.target_directory.replaceAll("/", "\\/")}`),
                                    path.join(full_prefix, router_name),
                                );
                                if (router_alias.include_target_middlewares) {
                                    new_handler.before_mounted_middlewares = [
                                        ...before_mounted_middlewares,
                                        ...(new_handler.before_mounted_middlewares || []),
                                    ];
                                    new_handler.middlewares = [...middlewares, ...(new_handler.middlewares || [])];
                                    new_handler.mounted_middlewares = [
                                        ...mounted_middlewares,
                                        ...(new_handler.mounted_middlewares || []),
                                    ];
                                } else {
                                    new_handler.before_mounted_middlewares = [...before_mounted_middlewares];
                                    new_handler.middlewares = [...middlewares];
                                    new_handler.mounted_middlewares = [...mounted_middlewares];
                                }
                                return new_handler;
                            });
                        handlers.push(...aliases);
                    });
                }
            }
        }
    }

    if (root) {
        await Promise.all(aliases.map((f) => f()));
    }
    root && log("finished", current_channels_directory);
}
