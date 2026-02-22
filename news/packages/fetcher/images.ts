const cache = new Map<string, string>();

export async function downloadImageAsBase64(url: string): Promise<string | null> {
    const hit = cache.get(url);
    if (hit) return hit;

    try {
        const res = await fetch(url);
        if (!res.ok) return null;

        let contentType = res.headers.get("content-type");
        if (!contentType?.startsWith("image/")) {
            const ext = url.split(".").pop()?.toLowerCase().split("?")[0];
            const mime: Record<string, string> = {
                jpg: "image/jpeg", jpeg: "image/jpeg",
                png: "image/png", gif: "image/gif",
                webp: "image/webp", svg: "image/svg+xml",
                bmp: "image/bmp", ico: "image/x-icon",
            };
            contentType = (ext && mime[ext]) ? mime[ext]! : null;
        }
        if (!contentType) return null;

        const buf = await res.arrayBuffer();
        const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)));
        const dataUrl = `data:${contentType};base64,${b64}`;
        cache.set(url, dataUrl);
        return dataUrl;
    } catch {
        return null;
    }
}