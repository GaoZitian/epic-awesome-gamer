# -*- coding: utf-8 -*-
# Time       : 2022/1/16 0:25
# Author     : QIN2DIM
# GitHub     : https://github.com/QIN2DIM
# Description: 游戏商城控制句柄

import json
from contextlib import suppress
from json import JSONDecodeError
from typing import List

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


URL_PROMOTIONS = "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions"
URL_PRODUCT_PAGE = "https://store.epicgames.com/en-US/p/"
URL_PRODUCT_BUNDLES = "https://store.epicgames.com/en-US/bundles/"


def get_promotions() -> List[PromotionGame]:
    """获取周免游戏数据"""
    def is_discount_game(prot: dict) -> bool | None:
        with suppress(KeyError, IndexError, TypeError):
            offers = prot["promotions"]["promotionalOffers"][0]["promotionalOffers"]
            for i, offer in enumerate(offers):
                if offer["discountSetting"]["discountPercentage"] == 0:
                    return True

    promotions: List[PromotionGame] = []

    resp = httpx.get(URL_PROMOTIONS, params={"local": "zh-CN"})

    try:
        data = resp.json()
    except JSONDecodeError as err:
        logger.error("Failed to get promotions", err=err)
        return []

    with suppress(Exception):
        cache_key = RUNTIME_DIR.joinpath("promotions.json")
        cache_key.parent.mkdir(parents=True, exist_ok=True)
        cache_key.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    # Get store promotion data and <this week free> games
    for e in data["data"]["Catalog"]["searchStore"]["elements"]:
        if not is_discount_game(e):
            continue

        # -----------------------------------------------------------
        # 🟢 智能 URL 识别逻辑
        # -----------------------------------------------------------
        is_bundle = False
        if e.get("offerType") == "BUNDLE":
            is_bundle = True
        
        # 补充检测：分类和标题
        if not is_bundle:
            for cat in e.get("categories", []):
                if "bundle" in cat.get("path", "").lower():
                    is_bundle = True
                    break
        if not is_bundle and "Collection" in e.get("title", ""):
             is_bundle = True

        base_url = URL_PRODUCT_BUNDLES if is_bundle else URL_PRODUCT_PAGE

        try:
            if e.get('offerMappings'):
                slug = e['offerMappings'][0]['pageSlug']
                e["url"] = f"{base_url.rstrip('/')}/{slug}"
            elif e.get("productSlug"):
                e["url"] = f"{base_url.rstrip('/')}/{e['productSlug']}"
            else:
                 e["url"] = f"{base_url.rstrip('/')}/{e.get('urlSlug', 'unknown')}"
        except (KeyError, IndexError):
            logger.info(f"Failed to get URL: {e}")
            continue

        logger.info(e["url"])
        promotions.append(PromotionGame(**e))

    return promotions


class EpicAgent:
    def __init__(self, page: Page):
        self.page = page
        self.epic_games = EpicGames(self.page)
        self._promotions: List[PromotionGame] = []
        self._ctx_cookies_is_available: bool = False
        self._orders: List[OrderItem] = []
        self._namespaces: List[str] = []
        self._cookies = None

    async def _sync_order_history(self):
        if self._orders:
            return
        completed_orders: List[OrderItem] = []
        try:
            await self.page.goto("https://www.epicgames.com/account/v2/payment/ajaxGetOrderHistory")
            text_content = await self.page.text_content("//pre")
            data = json.loads(text_content)
            for _order in data["orders"]:
                order = Order(**_order)
                if order.orderType != "PURCHASE":
                    continue
                for item in order.items:
                    if not item.namespace or len(item.namespace) != 32:
                        continue
                    completed_orders.append(item)
        except Exception as err:
            logger.warning(err)
        self._orders = completed_orders

    async def _check_orders(self):
        await self._sync_order_history()
        self._namespaces = self._namespaces or [order.namespace for order in self._orders]
        self._promotions = [p for p in get_promotions() if p.namespace not in self._namespaces]

    async def _should_ignore_task(self) -> bool:
        self._ctx_cookies_is_available = False
        await self.page.goto(URL_CLAIM, wait_until="domcontentloaded")
        status = await self.page.locator("//egs-navigation").get_attribute("isloggedin")
        if status == "false":
            logger.error("❌ context cookies is not available")
            return False
        self._ctx_cookies_is_available = True
        await self._check_orders()
        if not self._promotions:
            return True
        return False

    async def collect_epic_games(self):
        if await self._should_ignore_task():
            logger.success("All week-free games are already in the library")
            return

        if not self._ctx_cookies_is_available:
            return

        if not self._promotions:
            await self._check_orders()

        if not self._promotions:
            logger.success("All week-free games are already in the library")
            return

        for p in self._promotions:
            pj = json.dumps({"title": p.title, "url": p.url}, indent=2, ensure_ascii=False)
            logger.debug(f"Discover promotion \n{pj}")

        if self._promotions:
            try:
                await self.epic_games.collect_weekly_games(self._promotions)
            except Exception as e:
                logger.exception(e)
        
        logger.debug("All tasks in the workflow have been completed")


class EpicGames:
    def __init__(self, page: Page):
        self.page = page
        self._promotions: List[PromotionGame] = []

    @staticmethod
    def _is_quota_exhausted_error(err: Exception | str) -> bool:
        msg = str(err)
        keywords = ("RESOURCE_EXHAUSTED", "429", "quota", "billing details")
        return any(k in msg for k in keywords)

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
        wpc = page.frame_locator(iframe_selector).first

        logger.debug("Looking for 'PLACE ORDER' button...")
        place_order_btn = wpc.locator("button", has_text="PLACE ORDER")
        confirm_btn = wpc.locator("//button[contains(@class, 'payment-confirm__btn')]")
        
        try:
            await expect(place_order_btn).to_be_visible(timeout=15000)
            logger.debug("✅ Found 'PLACE ORDER' button via text match")
            return wpc, place_order_btn
        except AssertionError:
            pass
            
        try:
            await expect(confirm_btn).to_be_visible(timeout=5000)
            logger.debug("✅ Found button via CSS class match")
            return wpc, confirm_btn
        except AssertionError:
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

    async def _handle_instant_checkout(self, page: Page):
        logger.info("🚀 Triggering Instant Checkout Flow...")
        agent = AgentV(page=page, agent_config=settings)

        try:
            wpc, payment_btn = await self._active_purchase_container(page)
            logger.debug(f"Clicking payment button: {await payment_btn.text_content()}")
            await payment_btn.click(force=True)
            await page.wait_for_timeout(3000)
            
            try:
                logger.debug("Checking for CAPTCHA...")
                await agent.wait_for_challenge()
            except Exception as e:
                if self._is_quota_exhausted_error(e):
                    logger.error(
                        "Gemini quota exceeded during instant checkout captcha solving (429). "
                        "Abort current flow to avoid hammering the API."
                    )
                    raise
                logger.info(f"CAPTCHA detection skipped (Likely no CAPTCHA needed): {e}")

            try:
                if not await payment_btn.is_visible():
                     logger.success("🎉 Instant Checkout: Payment button disappeared (Success inferred)")
                     return
            except Exception:
                logger.success("🎉 Instant Checkout: Iframe closed (Success inferred)")
                return

            with suppress(Exception):
                await payment_btn.click(force=True)
                await page.wait_for_timeout(2000)
            
            logger.success("Instant checkout flow finished (Blind Success).")

        except Exception as err:
            if self._is_quota_exhausted_error(err):
                raise
            logger.warning(f"Instant checkout warning (Game might still be claimed): {err}")
            await page.reload()

    async def add_promotion_to_cart(self, page: Page, urls: List[str]) -> bool:
        has_pending_cart_items = False

        for url in urls:
            await page.goto(url, wait_until="load")

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

            # ------------------------------------------------------------
            # 🔥 新思路：彻底解决按钮识别问题 (黑名单机制 + 智能点击)
            # ------------------------------------------------------------
            
            # 1. 尝试找到所有可能的“主按钮”
            # Epic 按钮通常有 'purchase-cta-button' 这个 TestID
            purchase_btn = page.locator("//button[@data-testid='purchase-cta-button']").first

            # 2. 如果没找到主按钮，尝试找“库中”状态
            try:
                if not await purchase_btn.is_visible(timeout=5000):
                    # 再次检查是否在库中 (有时按钮不叫 purchase-cta，而是简单的 disabled button)
                    all_text = await page.locator("body").text_content()
                    if "In Library" in all_text or "Owned" in all_text:
                         logger.success(f"Already in the library (Page Text Scan) - {url=}")
                         continue
                    logger.warning(f"Could not find any purchase button - {url=}")
                    continue
            except Exception:
                pass

            # 3. 获取按钮文字
            btn_text = await purchase_btn.text_content()
            if not btn_text: btn_text = ""
            btn_text_upper = btn_text.strip().upper()
            
            logger.debug(f"👉 Found Button: '{btn_text}'")

            # 4. 黑名单检查：只有这些情况绝对不能点
            # 如果是 'IN LIBRARY', 'OWNED', 'UNAVAILABLE', 'COMING SOON' -> 跳过
            if any(s in btn_text_upper for s in ["IN LIBRARY", "OWNED", "UNAVAILABLE", "COMING SOON"]):
                logger.success(f"Game status is '{btn_text}' - Skipping.")
                continue

            # 5. 白名单检查 (Add to Cart 特殊处理)
            # 如果包含 'CART'，说明是加入购物车流程
            if "CART" in btn_text_upper:
                logger.debug(f"🛒 Logic: Add To Cart - {url=}")
                await purchase_btn.click()
                has_pending_cart_items = True
                continue
            
            # 6. 默认处理 (盲点逻辑)
            # 只要不是黑名单，也不是购物车，统统当做 "Get/Purchase" 直接点击！
            # 不管它写的是 'Get', 'Free', 'Purchase', 'Buy Now'，只要 API 说是免费的，我们就点！
            logger.debug(f"⚡️ Logic: Aggressive Click (Text: {btn_text}) - {url=}")
            await purchase_btn.click()
            
            # 点击后，转入即时结账流程
            await self._handle_instant_checkout(page)
            # ------------------------------------------------------------

        return has_pending_cart_items

    async def _empty_cart(self, page: Page, wait_rerender: int = 30) -> bool | None:
        has_paid_free = False
        try:
            cards = await page.query_selector_all("//div[@data-testid='offer-card-layout-wrapper']")
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
            await self.page.goto(URL_CART, wait_until="domcontentloaded")
            logger.debug("Move ALL paid games from the shopping cart out")
            await self._empty_cart(self.page)

            agent = AgentV(page=self.page, agent_config=settings)
            await self.page.click("//button//span[text()='Check Out']")
            await self._agree_license(self.page)

            try:
                logger.debug("Move to webPurchaseContainer iframe")
                wpc, payment_btn = await self._active_purchase_container(self.page)
                logger.debug("Click payment button")
                await self._uk_confirm_order(wpc)
                await agent.wait_for_challenge()
                return
            except Exception as err:
                if self._is_quota_exhausted_error(err):
                    logger.error(
                        "Gemini quota exceeded during checkout captcha solving (429). "
                        "Stop retries to avoid repeated quota errors."
                    )
                    raise

                logger.warning(f"Failed to solve captcha (attempt {attempt}/{max_attempts}) - {err}")
                if attempt >= max_attempts:
                    raise
                await self.page.reload()
                await self.page.wait_for_timeout(2000)

    @retry(retry=retry_if_exception_type(TimeoutError), stop=stop_after_attempt(2), reraise=True)
    async def collect_weekly_games(self, promotions: List[PromotionGame]):
        urls = [p.url for p in promotions]
        has_cart_items = await self.add_promotion_to_cart(self.page, urls)

        if has_cart_items:
            await self._purchase_free_game()
            try:
                await self.page.wait_for_url(URL_CART_SUCCESS)
                logger.success("🎉 Successfully collected cart games")
            except TimeoutError:
                logger.warning("Failed to collect cart games")
        else:
            logger.success("🎉 Process completed (Instant claimed or already owned)")
