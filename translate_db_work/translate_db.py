import json


def translate(raw_path: str):
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
            
    out_list = []

    for conversation in raw:
        conversationId = conversation.get("conversationId")
        tweets = [t for t in (conversation.get("tweets",[]))]
        if not conversationId or not tweets:
                out_list.append({"conversationId": conversationId, "threads": [], "hasMissingParent": False, "hasMultipleThreads": False})
                continue
        
        thread_list = []
        processed_ids = set()
        for index, tweet in enumerate(tweets):
            if tweet.get("replyCount") == 0:
                thread_list.append(tweet)
                break
            else:
                print('------------------------------------------------------')
                thread_list = [t.get("text") for t in tweets[:index+1]] + determine_descendants(tweet, tweets[index:])
                for t in thread_list:
                    print("\n"+t+"\n")
                print("\n\n\n")
                # get the starting point for each thread

def determine_descendants(original_tweet, tweets):
    if len(tweets) == 1:
        if tweets[0].get("inReplyToId") == original_tweet.get("id"):
            return [original_tweet.get("text"), tweets[0].get("text")]
        return [original_tweet.get("text")]

    threads = []
    original_tweet_id = original_tweet.get("id")
    for index, tweet in enumerate(tweets):
        t_id = tweet.get("id")
        t_reply_id = tweet.get("inReplyToId")
        
        if t_reply_id == original_tweet_id:
            threads = threads + [tweet.get("text")] +  (determine_descendants(tweets[index], tweets[index:]))
    return threads

translate("./raw.json")