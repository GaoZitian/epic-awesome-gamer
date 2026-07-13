# -*- coding: utf-8 -*-
# Time : 2022/1/16 0:25
# Author : QIN2DIM
# GitHub : https://github.com/QIN2DIM
# Description: 游戏商城控制句柄
import asyncio
import json
import os
import time
from contextlib import suppress
from enum import Enum
from json import JSONDecodeError
from typing import Any, List

import httpx
from hcaptcha_challenger.agent import AgentV
from loguru import logger
from playwright.async_api import Page
from playwright.async_api import expect, TimeoutError, FrameLocator
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from models import OrderItem, Order
from models import PromotionGame
from settings import settings, RUNTIME_DIR

URL_CLAIM = "https://store.epicgames.com/en-US/free-games"
URL_LOGIN = (
    f"https://www.epicgames.com/id/login?lang=en-US&noHostRedirect=true&redirectUrl={URL_CLAIM}"
)
URL_CART = "https://store.epicgames.com/en-US/cart"
URL_CART_SUCCESS = "https://store.epicgames.com/en-US/cart/success"
URL_ORDER_HISTORY = "https://www.epicgames.com/account/v2/payment/ajaxGetOrderHistory"

URL_PROMOTIONS = "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions"
URL_PRODUCT_PAGE = "https://store.epicgames.com/en-US/p/"
URL_PRODUCT_BUNDLES = "https://store.epicgames.com/en-US/bundles/"


class GameCollectResult(Enum):
    """游戏收集结果枚举"""

    ALL_OWNED = "all_owned"
    SUCCESS = "success"
    EULA_FAILED = "eula_failed"
    COOKIE_INVALID = "cookie_invalid"
    CAPTCHA_FAILED = "captcha_failed"
    CHECKOUT_FAILED = "checkout_failed"
    UNKNOWN_ERROR = "unknown_error"
    DRIVER_CRASH = "driver_crash"


class OrderHistoryUnavailable(RuntimeError):
    pass


def _promotion_count(data: dict) -> int:
    """统计促销数据中有效 namespace 数量"""
    namespaces: set[str] = set()
    with suppress(KeyError, TypeError):
        elements = data["data"]["Catalog"]["searchStore"]["elements"]
        for element in elements:
            with suppress(KeyError, IndexError, TypeError):
                offers = element["promotions"]["promotionalOffers"][0]["promotionalOffers"]
                if any(
                    offer["discountSetting"]["discountPercentage"] == 0
                    for offer in offers
                ):
                    namespace = element.get("namespace") or element.get("id")
                    if namespace:
                        namespaces.add(str(namespace))
    return len(namespaces)


def _fetch_promotions_data(expected_count: int = 0) -> dict:
    """带重试的促销数据获取，防止 Epic API 返回不完整数据"""
    best_data: dict = {}
    best_count = -1
    for attempt in range(1, 4):
        try:
            response = httpx.get(
                URL_PROMOTIONS,
                params={"local": "zh-CN"},
                timeout=30,
            )
            response.raise_for_status()
            candidate = response.json()
        except (httpx.HTTPError, JSONDecodeError, ValueError) as exc:
            logger.warning(
                f"promotion_fetch_error attempt={attempt} type={type(exc).__name__}"
            )
            if attempt < 3:
                time.sleep(1)
            continue
        count = _promotion_count(candidate)
        if count > best_count:
            best_data = candidate
            best_count = count
        if count >= expected_count:
            break
        if attempt < 3:
            time.sleep(1)
    return best_data


def _is_captcha_error(err: Exception | str) -> bool:
    message = str(err).lower()
    return any(
        keyword in message
        for keyword in (
            "captcha",
            "challenge",
            "hcaptcha",
            "getcaptcha",
            "payload to timeout",
            "valid challenge frame",
            "wait for captcha response timeout",
            "btoa",
            "read-only",
        )
    )


def _is_driver_disconnect_error(err: Exception | str) -> bool:
    message = str(err).lower()
    return any(
        keyword in message
        for keyword in (
            "connection closed while reading from the driver",
            "browsercontext.close: connection closed",
            "cannot read properties of undefined",
            "playwright/driver",
        )
    )


def _is_quota_exhausted_error(err: Exception | str) -> bool:
    msg = str(err)
    keywords = ("RESOURCE_EXHAUSTED", "429", "quota", "billing details")
    return any(k in msg for k in keywords)


async def _fetch_order_items(page: Page) -> List[OrderItem]:
    """通过 HTTP 请求获取订单历史，不离开当前页面上下文"""
    response = await page.context.request.get(
        URL_ORDER_HISTORY,
        headers={"Accept": "application/json"},
        timeout=15000,
    )
    status = response.status
    content_type = response.headers.get("content-type", "").lower()

    if status in {401, 403}:
        raise OrderHistoryUnavailable(f"session_invalid status={status}")
    if status == 429 or status >= 500:
        raise OrderHistoryUnavailable(f"transient status={status}")
    if status >= 400:
        raise OrderHistoryUnavailable(f"rejected status={status}")
    if "json" not in content_type:
        raise OrderHistoryUnavailable(f"unexpected_content_type status={status}")

    try:
        data = await response.json()
    except Exception as exc:
        raise OrderHistoryUnavailable(f"invalid_json status={status}") from exc

    completed_orders: List[OrderItem] = []
    for raw_order in data.get("orders", []):
        order = Order(**raw_order)
        if order.orderType != "PURCHASE":
            continue
        for item in order.items:
            if item.namespace and len(item.namespace) == 32:
                completed_orders.append(item)
    return completed_orders


def get_promotions() -> List[PromotionGame]:
    """获取周免游戏数据（带去重）"""

    def is_discount_game(prot: dict) -> bool | None:
        with suppress(KeyError, IndexError, TypeError):
            offers = prot["promotions"]["promotionalOffers"][0]["promotionalOffers"]
            for offer in offers:
                if offer["discountSetting"]["discountPercentage"] == 0:
                    return True

    promotions: List[PromotionGame] = []
    cache_key = RUNTIME_DIR.joinpath("promotions.json")
    expected_count = 0
    with suppress(Exception):
        expected_count = _promotion_count(
            json.loads(cache_key.read_text(encoding="utf-8"))
        )
    data = _fetch_promotions_data(expected_count)
    if not data:
        logger.error("获取促销信息失败: no valid response")
        return []

    with suppress(Exception):
        cache_key.parent.mkdir(parents=True, exist_ok=True)
        cache_key.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    # Get store promotion data
    for e in data["data"]["Catalog"]["searchStore"]["elements"]:
        if not is_discount_game(e):
            continue

        # 智能 URL 识别
        is_bundle = False
        if e.get("offerType") == "BUNDLE":
            is_bundle = True
        if not is_bundle:
            for cat in e.get("categories", []):
                if "bundle" in cat.get("path", "").lower():
                    is_bundle = True
                    break
        if not is_bundle and "Collection" in e.get("title", ""):
            is_bundle = True
        base_url = URL_PRODUCT_BUNDLES if is_bundle else URL_PRODUCT_PAGE

        try:
            if e.get("offerMappings"):
                slug = e["offerMappings"][0]["pageSlug"]
                e["url"] = f"{base_url.rstrip('/')}/{slug}"
            elif e.get("productSlug"):
                e["url"] = f"{base_url.rstrip('/')}/{e['productSlug']}"
            else:
                e["url"] = f"{base_url.rstrip('/')}/{e.get('urlSlug', 'unknown')}"
        except (KeyError, IndexError):
            logger.debug(f"Failed to get URL: {e}")
            continue

        logger.debug(f"发现周免游戏: {e['url']}")
        promotions.append(PromotionGame(**e))

    # Epic 可能对同一 namespace 暴露多个 offer，按 namespace 去重
    unique_promotions: dict[str, PromotionGame] = {}
    for promotion in promotions:
        unique_promotions.setdefault(promotion.namespace, promotion)
    return list(unique_promotions.values())


class EpicAgent:
    def __init__(self, page: Page):
        self.page = page
        self.epic_games = EpicGames(self.page)
        self._promotions: List[PromotionGame] = []
        self._ctx_cookies_is_available: bool = False
        self._orders: List[OrderItem] = []
        self._namespaces: List[str] = []
        self._cookies = None

    async def _handle_eula_correction(self) -> bool:
        """处理 EULA 修正页面"""
        current_url = self.page.url
        if "correction/eula" not in current_url and "corrective=" not in current_url:
            return False

        logger.warning("⚠️ 检测到 EULA 修正页面，尝试自动接受协议...")
        try:
            await self.page.wait_for_load_state("networkidle")
            await self.page.wait_for_timeout(2000)

            accept_selectors = [
                "#accept",
                "button#accept",
                "//button[@id='accept']",
                "//button[@type='submit']",
                "//button[normalize-space(text())='Accept']",
                "//button[normalize-space(text())='接受']",
            ]
            for selector in accept_selectors:
                try:
                    btn = self.page.locator(selector).first
                    if await btn.is_visible(timeout=5000):
                        btn_text = await btn.text_content()
                        logger.info(f"📋 点击 EULA 接受按钮: '{btn_text}'")
                        await btn.click()
                        await self.page.wait_for_load_state(
                            "networkidle", timeout=15000
                        )
                        if "correction/eula" not in self.page.url:
                            logger.success("✅ EULA 协议已接受")
                            return True
                        else:
                            logger.warning("⚠️ 仍在 EULA 页面，尝试下一个选择器")
                except Exception as e:
                    logger.debug(f"EULA 选择器 '{selector}' 失败: {e}")
                    continue
            logger.error("❌ 未能找到 EULA 接受按钮")
            return False
        except Exception as e:
            logger.error(f"❌ 处理 EULA 页面异常: {e}")
            return False

    async def _sync_order_history(self, force: bool = False):
        if self._orders and not force:
            return
        try:
            completed_orders = await _fetch_order_items(self.page)
        except Exception as err:
            logger.warning(err)
            completed_orders = []
        self._orders = completed_orders

    async def _check_orders(self, force: bool = False):
        await self._sync_order_history(force=force)
        self._namespaces = [order.namespace for order in self._orders]
        all_promotions = get_promotions()
        promotions = [
            p for p in all_promotions if p.namespace not in self._namespaces
        ]

        # 支持 EPIC_TARGET_GAMES_JSON 环境变量过滤
        raw_targets = os.getenv("EPIC_TARGET_GAMES_JSON", "").strip()
        if raw_targets:
            try:
                target_names = {
                    str(title).strip().casefold(): str(title).strip()
                    for title in json.loads(raw_targets)
                    if str(title).strip()
                }
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("invalid target game list") from exc

            known = {
                p.title.strip().casefold(): p
                for p in all_promotions
                if p.title.strip().casefold() in target_names
            }
            promotions = [
                p for p in promotions
                if p.title.strip().casefold() in target_names
            ]

        self._promotions = promotions

    async def _should_ignore_task(self) -> tuple[bool, GameCollectResult]:
        """检查是否应该忽略任务"""
        self._ctx_cookies_is_available = False
        await self.page.goto(URL_CLAIM, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(2000)  # 等待 JS 执行完成

        # EULA 修正页面检测
        max_eula_attempts = 3
        for attempt in range(max_eula_attempts):
            if "correction/eula" in self.page.url or "corrective=" in self.page.url:
                logger.warning(f"⚠️ 检测到修正页面（尝试 {attempt + 1}/{max_eula_attempts}）")
                if await self._handle_eula_correction():
                    await self.page.goto(URL_CLAIM, wait_until="domcontentloaded")
                    await self.page.wait_for_timeout(2000)
                else:
                    return False, GameCollectResult.EULA_FAILED
            else:
                break

        try:
            status = await self.page.locator("//egs-navigation").get_attribute(
                "isloggedin", timeout=10000
            )
        except Exception as e:
            if "correction" in self.page.url or "eula" in self.page.url:
                return False, GameCollectResult.EULA_FAILED
            logger.warning("Epic navigation marker unavailable; continuing with cookie")
            status = None

        if status == "false":
            logger.error("❌ Cookie 无效，账号未登录")
            return False, GameCollectResult.COOKIE_INVALID

        self._ctx_cookies_is_available = True
        await self._check_orders()
        if not self._promotions:
            return True, GameCollectResult.ALL_OWNED
        return False, GameCollectResult.SUCCESS

    async def collect_epic_games(self) -> GameCollectResult:
        """收集 Epic Games 周免游戏"""
        should_ignore, result = await self._should_ignore_task()

        if should_ignore:
            logger.success("✅ 所有周免游戏已在库中")
            return GameCollectResult.ALL_OWNED

        if result != GameCollectResult.SUCCESS:
            logger.error(f"❌ GAME_ERROR:{result.value}")
            return result

        if not self._promotions:
            await self._check_orders()
            if not self._promotions:
                logger.success("✅ 所有周免游戏已在库中")
                return GameCollectResult.ALL_OWNED

        for p in self._promotions:
            pj = json.dumps({"title": p.title, "url": p.url}, indent=2, ensure_ascii=False)
            logger.debug(f"Discover promotion\n{pj}")

        if self._promotions:
            try:
                await self.epic_games.collect_weekly_games(self._promotions)
            except Exception as e:
                logger.exception(e)
            logger.debug("All tasks in the workflow have been completed")

        return GameCollectResult.SUCCESS


class EpicGames:
    def __init__(self, page: Page):
        self.page = page
        self._promotions: List[PromotionGame] = []

    @staticmethod
    async def _agree_license(page: Page):
        logger.debug("Agree license")
        with suppress(TimeoutError):
            await page.click("//label[@for='agree']", timeout=4000)
        accept = page.locator("//button//span[text()='Accept']")
        if await accept.is_enabled():
            await accept.click()

    @staticmethod
    async def _active_purchase_container(page: Page):
        logger.debug("Scanning for purchase iframe...")
        iframe_selector = "//iframe[contains(@id, 'webPurchaseContainer') or contains(@src, 'purchase')]"

        # 先等 iframe 本身出现
        with suppress(TimeoutError):
            await page.wait_for_selector(iframe_selector, timeout=20000)

        wpc = page.frame_locator(iframe_selector).first
        logger.debug("Looking for checkout button in iframe...")

        # 更宽泛的按钮选择器：按优先级从精确到模糊
        # Epic 频繁改按钮文案和 class，必须兜底到"iframe 里任何可点按钮"
        candidates = [
            # 文案匹配（优先精确）
            ("PLACE ORDER", wpc.locator("button", has_text="PLACE ORDER")),
            ("COMPLETE ORDER", wpc.locator("button", has_text="COMPLETE ORDER")),
            ("CHECKOUT", wpc.locator("button", has_text="CHECKOUT")),
            ("CONFIRM", wpc.locator("button", has_text="CONFIRM")),
            ("PURCHASE", wpc.locator("button", has_text="PURCHASE")),
            # CSS class 匹配
            ("payment-confirm__btn", wpc.locator("button.payment-confirm__btn")),
            ("payment-confirm__button", wpc.locator("button.payment-confirm__button")),
            # type=submit
            ("submit", wpc.locator("button[type='submit']")),
            # input 类型按钮
            ("input-submit", wpc.locator("input[type='submit']")),
            # 最后兜底：iframe 里第一个可点击的 button（排除 disabled）
        ]

        for label, btn in candidates:
            try:
                await expect(btn).to_be_visible(timeout=15000)
                # 额外确认按钮不是 disabled
                if await btn.is_enabled(timeout=5000):
                    logger.debug(f"✅ Found '{label}' button")
                    return wpc, btn
            except Exception:
                continue

        # 兜底：iframe 里所有 button，找第一个 enabled 的
        try:
            all_buttons = wpc.locator("button")
            count = await all_buttons.count()
            logger.debug(f"Iframe contains {count} buttons, scanning...")
            for i in range(count):
                btn = all_buttons.nth(i)
                try:
                    if await btn.is_visible(timeout=3000):
                        if await btn.is_enabled(timeout=3000):
                            text = (await btn.text_content() or "").strip()[:50]
                            logger.debug(f"✅ Found fallback button [{i}] text='{text}'")
                            return wpc, btn
                except Exception:
                    continue
        except Exception:
            pass

        # 再兜底：iframe 里所有 input button
        try:
            all_inputs = wpc.locator("input[type='button'], input[type='submit']")
            count = await all_inputs.count()
            for i in range(count):
                inp = all_inputs.nth(i)
                try:
                    if await inp.is_visible(timeout=3000) and await inp.is_enabled(timeout=3000):
                        logger.debug(f"✅ Found fallback input button [{i}]")
                        return wpc, inp
                except Exception:
                    continue
        except Exception:
            pass

        logger.warning("Primary buttons not found in iframe.")
        raise AssertionError("Could not find Place Order button in iframe")

    @staticmethod
    async def _uk_confirm_order(wpc: FrameLocator):
        logger.debug("UK confirm order")
        with suppress(TimeoutError):
            accept = wpc.locator("//button[contains(@class, 'payment-confirm__btn')]")
            if await accept.is_enabled(timeout=5000):
                await accept.click()
                return True

    @staticmethod
    async def _click_product_cta(button: Any) -> None:
        """点击商品 CTA 按钮，带 fallback 策略"""
        try:
            await asyncio.wait_for(
                button.click(timeout=5000, no_wait_after=True), timeout=6
            )
            logger.debug("商品按钮常规点击完成")
            return
        except Exception as first_error:
            logger.warning(f"⚠️ 常规点击失败，尝试 force: {first_error}")
            try:
                await asyncio.wait_for(
                    button.click(force=True, timeout=5000, no_wait_after=True), timeout=6
                )
                logger.debug("商品按钮 force 点击完成")
            except Exception as force_error:
                raise RuntimeError(
                    f"checkout cta click failed: {force_error}"
                ) from force_error

    @staticmethod
    async def _wait_for_checkout_surface(page: Page, timeout_ms: int = 60000) -> str:
        """等待结账界面出现（iframe 或购买按钮）"""
        selectors = [
            ("//iframe[contains(@id, 'webPurchaseContainer') or contains(@src, 'purchase')]", "iframe"),
            ("button[data-testid='purchase-cta-button']", "cta"),
        ]
        for sel, label in selectors:
            try:
                await page.wait_for_selector(sel, timeout=timeout_ms)
                return label
            except TimeoutError:
                continue
        raise TimeoutError("no checkout surface appeared")

    async def _handle_instant_checkout(self, page: Page):
        logger.info("🚀 Triggering Instant Checkout Flow...")

        try:
            wpc, payment_btn = await self._active_purchase_container(page)
            logger.debug(f"Clicking payment button: {await payment_btn.text_content()}")
            await payment_btn.click(force=True)
            await page.wait_for_timeout(3000)

            # 尝试 CAPTCHA 检测
            try:
                logger.debug("Checking for CAPTCHA...")
                agent = AgentV(page=page, agent_config=settings)
                await agent.wait_for_challenge()
            except Exception as e:
                if _is_quota_exhausted_error(e):
                    logger.error(
                        "Gemini quota exceeded (429). "
                        "Abort to avoid hammering the API."
                    )
                    raise
                if _is_captcha_error(e):
                    logger.warning(f"CAPTCHA encountered but skipped: {e}")
                else:
                    logger.info(f"CAPTCHA check skipped: {e}")

            # 推断成功：按钮消失或 iframe 关闭
            try:
                if not await payment_btn.is_visible():
                    logger.success("🎉 Payment button disappeared (Success inferred)")
                    return
            except Exception:
                logger.success("🎉 Iframe closed (Success inferred)")
                return

            with suppress(Exception):
                await payment_btn.click(force=True)
                await page.wait_for_timeout(2000)

            logger.success("Instant checkout flow finished (Blind Success).")

        except Exception as err:
            if _is_quota_exhausted_error(err):
                raise
            logger.warning(f"Instant checkout warning: {err}")
            # 用 ESC 温和恢复，不用 page.reload()
            try:
                await page.keyboard.press("Escape", timeout=3000)
                await page.wait_for_timeout(2000)
            except Exception:
                pass
            raise

    async def add_promotion_to_cart(self, page: Page, urls: List[str]) -> bool:
        has_pending_cart_items = False

        for url in urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except TimeoutError:
                logger.warning(f"Page load timeout for {url}, trying continue anyway...")

            # 404 检测
            title = await page.title()
            if "404" in title or "Page Not Found" in title:
                logger.error(f"❌ Invalid URL (404 Page): {url}")
                continue

            # 处理年龄限制弹窗
            try:
                continue_btn = page.locator("//button//span[text()='Continue']")
                if await continue_btn.is_visible(timeout=5000):
                    await continue_btn.click()
            except Exception:
                pass

            # 找主按钮
            purchase_btn = page.locator(
                "//button[@data-testid='purchase-cta-button']"
            ).first

            try:
                if not await purchase_btn.is_visible(timeout=5000):
                    all_text = await page.locator("body").text_content()
                    if "In Library" in all_text or "Owned" in all_text:
                        logger.success(f"Already in library - {url=}")
                        continue
                    logger.warning(f"Could not find purchase button - {url=}")
                    continue
            except Exception:
                pass

            btn_text = await purchase_btn.text_content()
            if not btn_text:
                btn_text = ""
            btn_text_upper = btn_text.strip().upper()
            logger.debug(f"👉 Found Button: '{btn_text}'")

            # 黑名单
            if any(
                s in btn_text_upper
                for s in ["IN LIBRARY", "OWNED", "UNAVAILABLE", "COMING SOON"]
            ):
                logger.success(f"Skipping '{btn_text}'")
                continue

            # 白名单：购物车
            if "CART" in btn_text_upper:
                logger.debug(f"🛒 Add To Cart - {url=}")
                await purchase_btn.click()
                has_pending_cart_items = True
                continue

            # 默认：直接点击（用改进的点击方法）
            logger.debug(f"⚡️ Aggressive Click ({btn_text}) - {url=}")
            try:
                await self._click_product_cta(purchase_btn)
                await self._handle_instant_checkout(page)
            except Exception as e:
                if _is_quota_exhausted_error(e):
                    raise
                logger.warning(f"Instant checkout failed for {url}, "
                               f"will try cart path - {e}")
                has_pending_cart_items = True

        return has_pending_cart_items

    async def _empty_cart(self, page: Page, wait_rerender: int = 30) -> bool | None:
        has_paid_free = False
        try:
            cards = await page.query_selector_all(
                "//div[@data-testid='offer-card-layout-wrapper']"
            )
            for card in cards:
                is_free = await card.query_selector("//span[text()='Free']")
                if not is_free:
                    has_paid_free = True
                wishlist_btn = await card.query_selector(
                    "//button//span[text()='Move to wishlist']"
                )
                await wishlist_btn.click()
            if has_paid_free and wait_rerender:
                wait_rerender -= 1
                await page.wait_for_timeout(2000)
                return await self._empty_cart(page, wait_rerender)
            return True
        except TimeoutError as err:
            logger.warning("Failed to empty shopping cart", err=err)
            return False

    async def _purchase_free_game(self, max_attempts: int = 3):
        for attempt in range(1, max_attempts + 1):
            try:
                await self.page.goto(URL_CART, wait_until="domcontentloaded",
                                     timeout=30000)
            except TimeoutError:
                logger.warning(f"Cart page load timeout attempt {attempt}/{max_attempts}")

            logger.debug("Move ALL paid games from cart")
            await self._empty_cart(self.page)

            # "Check Out" 按钮选择器：Epic 频繁改文案，必须兜底
            checkout_selectors = [
                "//button//span[text()='Check Out']",
                "//button//span[text()='Proceed to Checkout']",
                "//button//span[text()='Proceed']",
                "//button//span[text()='Buy']",
                "//button[text()='Check Out']",
                "//button[text()='Proceed to Checkout']",
                "//button[text()='Proceed']",
                "//button[text()='Buy']",
                "//button[@data-testid='cart-checkout-button']",
                "//button[contains(@class, 'checkout-button')]",
                "//button[contains(@class, 'cart-checkout')]",
                "//a[contains(@class, 'checkout-button')]",
            ]

            checkout_clicked = False
            for selector in checkout_selectors:
                try:
                    logger.debug(f"Trying checkout selector: {selector}")
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=5000):
                        await btn.click(timeout=5000, no_wait_after=True)
                        logger.debug(f"✅ Checkout clicked via: {selector}")
                        checkout_clicked = True
                        await page.wait_for_timeout(2000)
                        break
                except Exception:
                    continue

            if not checkout_clicked:
                # 兜底：找购物车页面上任何包含 checkout 相关文字的按钮
                logger.warning("No standard checkout button found, scanning for any checkout-like button...")
                try:
                    all_buttons = page.locator("button, a")
                    count = await all_buttons.count()
                    for i in range(count):
                        btn = all_buttons.nth(i)
                        try:
                            if await btn.is_visible(timeout=1000):
                                text = (await btn.text_content() or "").strip().upper()
                                if any(kw in text for kw in ["CHECKOUT", "PROCEED", "BUY", "PURCHASE", "CONFIRM"]):
                                    if await btn.is_enabled(timeout=1000):
                                        await btn.click(timeout=5000, no_wait_after=True)
                                        logger.debug(f"✅ Fallback checkout clicked: '{text}'")
                                        checkout_clicked = True
                                        await page.wait_for_timeout(2000)
                                        break
                        except Exception:
                            continue
                except Exception:
                    pass

            if not checkout_clicked:
                logger.error("Could not find any checkout button on cart page")
                raise RuntimeError("No checkout button found")

            await self._agree_license(self.page)

            # CAPTCHA solver agent
            agent = AgentV(page=self.page, agent_config=settings)

            try:
                logger.debug("Move to webPurchaseContainer iframe")
                wpc, payment_btn = await self._active_purchase_container(self.page)
                logger.debug("Click payment button")
                await self._uk_confirm_order(wpc)
                await agent.wait_for_challenge()
                return
            except Exception as err:
                if _is_quota_exhausted_error(err):
                    logger.error(
                        "Gemini quota exceeded during checkout captcha (429). "
                        "Stop retries."
                    )
                    raise
                logger.warning(
                    f"Failed captcha (attempt {attempt}/{max_attempts}) - {err}"
                )
                if attempt >= max_attempts:
                    raise
                # 用温和恢复替代 page.reload()
                try:
                    await self.page.keyboard.press("Escape", timeout=3000)
                except Exception:
                    pass
                await self.page.wait_for_timeout(2000)

    @retry(
        retry=retry_if_exception_type(TimeoutError),
        stop=stop_after_attempt(2),
        reraise=True,
    )
    async def collect_weekly_games(self, promotions: List[PromotionGame]):
        """逐个领取游戏，单个失败不影响其他"""
        collected = []
        for promo in promotions:
            try:
                logger.info(f"Processing game: {promo.title} - {promo.url}")
                urls = [promo.url]
                has_cart_items = await self.add_promotion_to_cart(self.page, urls)
                if has_cart_items:
                    await self._purchase_free_game()
                    try:
                        await self.page.wait_for_url(URL_CART_SUCCESS, timeout=15000)
                        logger.success("🎉 Successfully collected cart games")
                    except TimeoutError:
                        logger.warning(
                            "Failed to navigate to success page"
                        )
                else:
                    logger.success(
                        f"🎉 {promo.title} (Instant claimed or already owned)"
                    )
                collected.append(promo.title)
            except Exception as e:
                if _is_quota_exhausted_error(e):
                    raise  # quota 错误不吞
                if _is_driver_disconnect_error(e):
                    raise  # 驱动断开需上层处理
                logger.exception(f"❌ Failed to collect {promo.title}, skipping...")
                continue

        if collected:
            logger.success(
                f"✅ Collected {len(collected)} game(s): {', '.join(collected)}"
            )
        else:
            logger.warning("⚠️ No games were collected this run")
